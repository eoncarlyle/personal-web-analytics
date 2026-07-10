import Deserialise.nginxLogDeserialiser
import Model.{AppEnvironment, CirceError, DevEnvironment, NginxLog, ProdEnvironment, getEnvironment}
import cats.effect.IO.traverse_
import cats.effect.kernel.{Clock, Resource, Temporal}
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits.{toFoldableOps, toTraverseOps}
import cats.syntax.traverse
import com.typesafe.config.{Config, ConfigFactory}
import fs2._
import fs2.kafka._
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import doobie._
import doobie.implicits._
import doobie.util.transactor
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import doobie.util.transactor.Strategy
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{Delete, DeleteObjectsRequest, ListObjectsV2Request, ObjectIdentifier, PutObjectRequest, S3Object}
import software.amazon.awssdk.core.sync.RequestBody

import scala.jdk.CollectionConverters
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.time.Instant
import java.time.temporal.TemporalUnit
import scala.concurrent.duration.{DAYS, Duration, DurationInt}
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IterableHasAsJava}
import scala.util.matching.Regex

object Main extends IOApp {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private def getConsumerConfig(config: Config, appEnvironment: AppEnvironment): ConsumerSettings[IO, Option[String], Either[CirceError, NginxLog]] = {
    val kafka = config.getConfig("kafka")
    val ssl = kafka.getConfig("ssl")

    val configFileSettings = Seq(
      "security.protocol" -> ssl.getString("security-protocol"),
      "ssl.keystore.type" -> ssl.getString("keystore-type"),
      "ssl.keystore.location" -> ssl.getString("keystore-location"),
      "ssl.keystore.password" -> ssl.getString("keystore-password"),
      "ssl.key.password" -> ssl.getString("key-password"),
      "ssl.truststore.type" -> ssl.getString("truststore-type"),
      "ssl.truststore.location" -> ssl.getString("truststore-location"),
      "ssl.truststore.password" -> ssl.getString("truststore-password"),
      "ssl.protocol" -> ssl.getString("protocol"),
      "ssl.enabled.protocols" -> ssl.getString("enabled-protocols"),
    )

    val groupId = appEnvironment match {
      case DevEnvironment => "dev-personal-web-analytics"
      case ProdEnvironment => "prod-personal-web-analytics"
    }

    val defaultConsumerSettings =
      ConsumerSettings[IO, Option[String], Either[CirceError, NginxLog]]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(kafka.getString("bootstrap-servers"))
        .withGroupId(groupId)

    configFileSettings.foldLeft(defaultConsumerSettings)((setting, fileSetting) => setting.withProperty(fileSetting._1, fileSetting._2))
  }

  private type Transactor = transactor.Transactor.Aux[IO, Unit]

  private def getSqliteConfig(config: Config) = config.getString("database")

  private def getTransactor(config: Config): Transactor = Transactor.fromDriverManager[IO](
    driver = "org.sqlite.JDBC", url = s"jdbc:sqlite:${getSqliteConfig(config)}", user = "", password = "", logHandler = None
  )

  private def getS3Resource: Resource[IO, S3Client] =
    Resource.fromAutoCloseable(
      IO(S3Client.builder()
        .credentialsProvider(DefaultCredentialsProvider.builder().build())
        .endpointOverride(URI.create("https://t3.storage.dev"))
        .region(Region.of("auto")).build())
    )

  private type AppConsumerRecord = ConsumerRecord[Option[String], Either[CirceError, NginxLog]]

  private def insertLog(nginxLog: NginxLog, appConsumerRecord: AppConsumerRecord) = {
    val NginxLog(serverName, uri, remoteAddr, referrer) = nginxLog
    for {
      _ <-
        sql"""
          INSERT INTO nginx_log (server_name, uri, remote_addr, referrer)
          VALUES ($serverName, $uri, $remoteAddr, ${referrer.getOrElse("")})
          ON CONFLICT (server_name, uri, remote_addr, referrer)
          DO UPDATE SET count = count + 1;
        """.update.run
      _ <-
        sql"""
          INSERT OR REPLACE INTO consumer_state (partition, offset)
          VALUES (${appConsumerRecord.partition}, ${appConsumerRecord.offset})
        """.update.run
    } yield ()
  }

  private def consumeRecords(transactor: Transactor)(records: Chunk[ConsumerRecord[Option[String], Either[CirceError, NginxLog]]]) =
    Logger[IO].info(s"Consumer fetched ${records.size} records") *> records.traverse { record =>
      record.value match {
        case Left(_) => Logger[IO].info(s"Consumer fetched ${records.size} records")
        case Right(nginxLog) => insertLog(nginxLog, record).transact(transactor)
      }
    }.as(CommitNow)

  private def getEnvSlug(appEnvironment: AppEnvironment) = appEnvironment match {
    case DevEnvironment => "dev"
    case ProdEnvironment => "prod"
  }

  private def getConfigFileName(appEnvironment: AppEnvironment) = appEnvironment match {
    case DevEnvironment => "dev.application.conf"
    case ProdEnvironment => "prod.application.conf"
  }


  private val s3BackupObjectRegex: Regex = "^([0-9]+)\\..*$".r

  private def getBackupFileRegex(appEnvironment: AppEnvironment): Regex = s"^([0-9]+)\\.${getEnvSlug(appEnvironment)}.backup.app.sqlite".r

  private def getBackupObjectsWithCreationTime(s3Objects: List[S3Object]): Iterable[(S3Object, Instant)] = s3Objects.sortBy(o => o.key())
    .flatMap(o => s3BackupObjectRegex.findAllMatchIn(o.key())
      .map(m => Instant.ofEpochSecond(m.group(1).toLong)).nextOption().map(ts => (o, ts)))

  private def s3ToDelete(backupObjectWithCreationTime: List[(S3Object, Instant)], current: Instant, interval: Duration, bucketSize: Duration, keepOldestObject: Boolean): Iterable[S3Object] = {
    val bucketedObjects = backupObjectWithCreationTime.groupBy(p => (current.getEpochSecond - p._2.getEpochSecond) / bucketSize.toSeconds)

    if (bucketedObjects.isEmpty) {
      List.empty
    } else {
      val maxBucket = bucketedObjects.keySet.max
      val bucketCeiling = interval.toSeconds / bucketSize.toSeconds
      bucketedObjects.flatMap(entry => {
        val (bucket, entries) = entry
        (bucket, keepOldestObject) match {
          case (bucket, true) if bucket == maxBucket && bucket < bucketCeiling => entries.sortBy(_._2).tail.map(_._1)
          case (bucket, false) if bucket > bucketCeiling => entries.map(_._1)
          case _ => entries.sortBy(_._2)(Ordering.by[Instant, Long](-1 * _.getEpochSecond)).tail.map(_._1)
        }
      }
      )
    }
  }

  private def toObjectIdentifiers(o: S3Object) = ObjectIdentifier.builder().key(o.key()).build()

  private def getDeleteObjectsRequest(backupBucket: String, objectIdentifiers: java.util.Collection[ObjectIdentifier]) = DeleteObjectsRequest.builder().bucket(backupBucket).delete(Delete.builder().objects(objectIdentifiers).build()).build()

  // Bucket it like: weekly backups up to a month
  // Up to 4 backups in last day
  private def cleanupBackups(currentTime: Instant, s3Client: S3Client, backupBucket: String, backupFileRegex: Regex, dataDirectory: URI) = for {
    _ <- Logger[IO].info("Cleaning up database backups on disk")
    onDisk = IO.blocking(Files.list(Path.of(dataDirectory)).filter(p => backupFileRegex.matches(p.getFileName.toString)).toList.asScala.toList)
    _ <- onDisk.flatMap { files =>
        Logger[IO].info(s"Deleting ${files.size} database backups on disk") *>
        files.traverse_(p => IO.blocking(Files.delete(p)))
    }
    _ <- Logger[IO].info("Cleaning up database backups in object storage")
    s3BackupsListResponse <- IO.blocking(s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(backupBucket).build()))
    s3BackupObjects = getBackupObjectsWithCreationTime(s3BackupsListResponse.contents().asScala.toList)
    (youngerThanDay, olderThanDay) = s3BackupObjects.partition(_._2.isAfter(currentTime.minus(1, DAYS.toChronoUnit)))

    // Backups younger than 1 day: at least one backup needs to be 'on track' to progress to the 30-day window, hence `keepOldestObject` value
    youngerThanDayToDelete = s3ToDelete(youngerThanDay.toList, currentTime, 1.days, 4.hours, keepOldestObject = true).map(toObjectIdentifiers).asJavaCollection
    olderThanDayToDelete = s3ToDelete(olderThanDay.toList, currentTime, 30.days, 5.days, keepOldestObject = false).map(toObjectIdentifiers).asJavaCollection
    _ <- Logger[IO].info(s"Deleting ${olderThanDayToDelete.size} S3 backups older than one day")
    _ <- IO.blocking(s3Client.deleteObjects(getDeleteObjectsRequest(backupBucket, olderThanDayToDelete)))
    _ <- Logger[IO].info(s"Deleting ${youngerThanDayToDelete.size} S3 backups younger than one day")
    _ <- IO.blocking(s3Client.deleteObjects(getDeleteObjectsRequest(backupBucket, youngerThanDayToDelete)))


  } yield ()

  private def backup(appEnvironment: AppEnvironment, s3Client: S3Client, transactor: Transactor, backupBucket: String, dataDirectory: URI) = ((for {
    _ <- Logger[IO].info("Backing up tracking database")
    currentTime <- IO.realTimeInstant
    _ <- cleanupBackups(currentTime, s3Client, backupBucket, getBackupFileRegex(appEnvironment), dataDirectory)
    //TODO: cleanup on-disk
    backupName = s"${currentTime.getEpochSecond.toString}.${getEnvSlug(appEnvironment)}.backup.app.sqlite"
    _ <-
      sql"""
          VACUUM INTO ${backupName}
        """.update.run.transact(Transactor.strategy.set(transactor, Strategy.void))
    _ <- IO.blocking(s3Client.putObject(
      PutObjectRequest.builder().bucket(backupBucket).key(backupName).build(),
      RequestBody.fromFile(Path.of(dataDirectory.resolve(backupName)))))
    _ <- Logger[IO].info("Backup succeeded")
  } yield ()) >> IO.sleep(10.minutes)).foreverM

  override def run(args: List[String]): IO[ExitCode] = {

    val maybeEnvironment: Option[AppEnvironment] = args match {
      case single :: Nil => Some(single).flatMap(getEnvironment)
      case _ => None
    }

    maybeEnvironment.map(environment => {
      for {
        baseConfig <- IO(ConfigFactory.parseResourcesAnySyntax(getConfigFileName(environment)))
        transactor = getTransactor(baseConfig)
        consumerConfig = getConsumerConfig(baseConfig, environment)
        nginxLogsTopic = baseConfig.getString("nginx-logs-topic")
        backupBucket = baseConfig.getString("backup-bucket")
        dataDirectory = Paths.get(getSqliteConfig(baseConfig)).getParent.toUri;
        _ <- getS3Resource.use(s3 => backup(environment, s3, transactor, backupBucket, dataDirectory)).start
        _ <- KafkaConsumer.stream(consumerConfig).subscribeTo(nginxLogsTopic).consumeChunk(consumeRecords(transactor)(_))
      } yield ExitCode.Success
    }).getOrElse(Logger[IO].error("Must specify application environment as `dev` or `prod`") *> IO(ExitCode.Error))
  }
}