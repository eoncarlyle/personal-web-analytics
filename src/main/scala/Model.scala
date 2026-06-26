object Model {
  case class NginxLog(serverName: String, uri: String, remoteAddress: String, referrer: Option[String])
  type CirceError = io.circe.Error

  sealed trait AppEnvironment
  case object DevEnvironment extends  AppEnvironment
  case object ProdEnvironment extends AppEnvironment

  def getEnvironment(maybeEnvironment: String): Option[AppEnvironment] =
    maybeEnvironment match {
      case "dev" => Some(DevEnvironment)
      case "prod" => Some(ProdEnvironment)
      case _ => None
    }
}
