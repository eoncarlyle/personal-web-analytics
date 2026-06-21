import io.circe.Decoder
import fs2.kafka.Deserializer
import cats.effect.IO
import io.circe.jawn.decode


case class NginxLog(serverName: String, uri: String, status: Int, remoteAddress: String, referrer: Option[String])

object Deserialise {

  implicit val optionalString: Decoder[Option[String]] =
    Decoder.decodeString.map(s => Option(s).filter(_.nonEmpty))

  implicit val decodeNginxLog: Decoder[NginxLog] =
    Decoder.forProduct5("server_name", "uri", "status", "remote_addr", "http_referrer")(NginxLog.apply)

  def kafkaValueDeserialiser[A: Decoder]: Deserializer[IO, A] = Deserializer.lift(bytes => {
      val stringVal = new String(bytes, "UTF-8")
      println(stringVal)

      for {
        a <- IO.fromEither(decode[A](new String(bytes, "UTF-8"))).onError(e => IO.println(e))
      } yield  a
  })

  implicit val nginxLogDeserialiser: Deserializer[IO, NginxLog] = kafkaValueDeserialiser[NginxLog]
}