import Deserialise.nginxLogDeserialiser
import Model.{CirceError, NginxLog}
import cats.effect.{IO, IOApp}
import com.typesafe.config.{Config, ConfigFactory}
import fs2._
import fs2.kafka._
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import doobie._
import doobie.implicits._
import cats.effect.kernel.Resource
import doobie.util.transactor

object Main extends IOApp.Simple {

  def getConsumerConfig(config: Config): ConsumerSettings[IO, Option[String], Either[CirceError, NginxLog]] = {
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

    val defaultConsumerSettings =
      ConsumerSettings[IO, Option[String], Either[CirceError, NginxLog]]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(kafka.getString("bootstrap-servers"))
        .withGroupId("personal-web-analytics")

    configFileSettings.foldLeft(defaultConsumerSettings)((setting, fileSetting) => setting.withProperty(fileSetting._1, fileSetting._2))
  }

  // TODO WAL pragma
  def getSqliteConfig(config: Config) = config.getString("database")

  private type Transactor = transactor.Transactor.Aux[IO, Unit]

  def getTransactor(config: Config): Transactor = Transactor.fromDriverManager[IO](
    driver = "org.sqlite.JDBC", url = s"jdbc:sqlite:${getSqliteConfig(config)}", user = "", password = "", logHandler = None
  )

  def insertLog(nginxLog: NginxLog): Update0 = {
    val NginxLog(serverName, uri, remoteAddr, referrer) = nginxLog
    sql"""
    INSERT INTO nginx_log (server_name, uri, remote_addr, referrer)
    VALUES ($serverName, $uri, $remoteAddr, ${referrer.getOrElse("")})
    ON CONFLICT (server_name, uri, remote_addr, referrer)
    DO UPDATE SET count = count + 1;
    """.update
  }

  private def consumeRecords(transactor: Transactor)(records: Chunk[ConsumerRecord[Option[String], Either[CirceError, NginxLog]]]) =
    records.traverse { record =>
      record.value match {
        case Left(_) => IO.pure(-1)
        case Right(nginxLog) => insertLog(nginxLog).run.transact(transactor)
      }
    }.as(CommitNow)

  //records.traverse { record =>
  //  match eitherNginxLog.value with
  //  case Left(error) => IO.pure(-1)
  //  case Right(nginxLog) => insertLog(nginxLog).run.transact(transactor)
  //}.as(CommitNow)

  //    records.traverse(record => IO.println(s"${record.value}")).as(CommitNow)

  val run: IO[Unit] = {
    for {
      baseConfig <- IO(ConfigFactory.load())
      transactor = getTransactor(baseConfig)
      consumerConfig = getConsumerConfig(baseConfig)
      nginxLogsTopic = baseConfig.getString("nginx-logs-topic")
      _ <- KafkaConsumer.stream(consumerConfig).subscribeTo(nginxLogsTopic).consumeChunk(consumeRecords(transactor))
    } yield ()
  }
}