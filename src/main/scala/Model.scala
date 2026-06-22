object Model {
  case class NginxLog(serverName: String, uri: String, remoteAddress: String, referrer: Option[String])
  type CirceError = io.circe.Error
}
