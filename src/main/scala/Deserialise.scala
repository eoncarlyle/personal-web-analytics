import Model.{CirceError, NginxLog}
import io.circe.Decoder
import fs2.kafka.Deserializer
import cats.effect.IO
import io.circe.jawn.decode

object Deserialise {

  implicit val optionalString: Decoder[Option[String]] =
    Decoder.decodeString.map(s => Option(s).filter(_.nonEmpty))

  implicit val decodeNginxLog: Decoder[NginxLog] =
    Decoder.forProduct4("server_name", "uri", "remote_addr", "http_referrer")(NginxLog.apply)

  def kafkaValueDeserialiser[A: Decoder]: Deserializer[IO, Either[CirceError, A]] = Deserializer.lift { bytes => IO(decode[A](new String(bytes, "UTF-8"))) }

  implicit val nginxLogDeserialiser: Deserializer[IO, Either[CirceError, NginxLog]] = kafkaValueDeserialiser[NginxLog]
}