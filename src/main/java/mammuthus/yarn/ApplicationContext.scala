package mammuthus.yarn


import mammuthus.yarn.log.Logging
import mammuthus.yarn.exception.ApplicationException
import mammuthus.yarn.util.Utils


/**
 * 8/20/15 WilliamZhu(allwefantasy@gmail.com)
 */
class ApplicationContext(config: ApplicationConf) extends Logging {
  val startTime = System.currentTimeMillis()

  @volatile private var stopped: Boolean = false

  private def assertNotStopped(): Unit = {
    if (stopped) {
      throw new IllegalStateException("Cannot call methods on a stopped SparkContext")
    }
  }

  val conf = config.clone()

  def getConf: ApplicationConf = conf.clone()

  if (!conf.contains("mammuthus.master")) {
    throw new ApplicationException("A master URL must be set in your configuration")
  }
  if (!conf.contains("mammuthus.app.name")) {
    throw new ApplicationException("An application name must be set in your configuration")
  }

  if (conf.getBoolean("mammuthus.logConf", false)) {
    logInfo("mammuthus configuration:\n" + conf.toDebugString)
  }

  // Set Spark driver host and port system properties
  conf.setIfMissing("mammuthus.driver.host", Utils.localHostName())
  conf.setIfMissing("mammuthus.driver.port", "0")

  val jars: Seq[String] =
    conf.getOption("mammuthus.jars").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

  val files: Seq[String] =
    conf.getOption("mammuthus.files").map(_.split(",")).map(_.filter(_.size != 0)).toSeq.flatten

  val master = conf.get("mammuthus.master")
  val appName = conf.get("mammuthus.app.name")
}
object ApplicationContext extends Logging {
  def jarOfClass(cls: Class[_]): Option[String] = {
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    if (uri != null) {
      val uriStr = uri.toString
      if (uriStr.startsWith("jar:file:")) {
        // URI will be of the form "jar:file:/path/foo.jar!/package/cls.class",
        // so pull out the /path/foo.jar
        Some(uriStr.substring("jar:file:".length, uriStr.indexOf('!')))
      } else {
        None
      }
    } else {
      None
    }
  }
}
