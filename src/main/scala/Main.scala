import Deserialise.nginxLogDeserialiser
import Model.{AppEnvironment, CirceError, DevEnvironment, NginxLog, ProdEnvironment, getEnvironment}
import cats.effect.{ExitCode, IO, IOApp}
import com.typesafe.config.{Config, ConfigFactory}
import fs2._
import fs2.kafka._
import fs2.kafka.consumer.KafkaConsumeChunk.CommitNow
import doobie._
import doobie.implicits._
import doobie.util.transactor
import org.typelevel.log4cats.{Logger, LoggerFactory}
import org.typelevel.log4cats.slf4j.{Slf4jFactory, Slf4jLogger}

import java.io.File

object Main extends IOApp {
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  def getConsumerConfig(config: Config, appEnvironment: AppEnvironment): ConsumerSettings[IO, Option[String], Either[CirceError, NginxLog]] = {
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
    Logger[IO].info(s"Consumer fetched ${records.size} records") *> records.traverse { record =>
      record.value match {
        case Left(_) => Logger[IO].info(s"Consumer fetched ${records.size} records")
        case Right(nginxLog) => insertLog(nginxLog).run.transact(transactor)
      }
    }.as(CommitNow)

  private def getConfigFileName(appEnvironment: AppEnvironment) = appEnvironment match {
    case DevEnvironment => "dev.application.conf"
    case ProdEnvironment => "prod.application.conf"
  }

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
        _ <- KafkaConsumer.stream(consumerConfig).subscribeTo(nginxLogsTopic).consumeChunk(consumeRecords(transactor))
      } yield ExitCode.Success
    }).getOrElse(Logger[IO].error("Must specify application environment as `dev` or `prod`") *> IO(ExitCode.Error))
  }
}