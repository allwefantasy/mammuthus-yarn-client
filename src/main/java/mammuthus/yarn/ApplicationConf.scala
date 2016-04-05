package mammuthus.yarn

import mammuthus.yarn.log.Logging
import java.util.concurrent.ConcurrentHashMap
import mammuthus.yarn.util.Utils
import scala.collection.JavaConverters._


/**
 * 8/20/15 WilliamZhu(allwefantasy@gmail.com)
 */
class ApplicationConf(loadDefaults: Boolean) extends Cloneable with Logging {
  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  if (loadDefaults) {
    // Load any spark.* system properties
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("mammuthus.")) {
      set(key, value)
    }
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): ApplicationConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(translateConfKey(key, warn = true), value)
    this
  }

  def translateConfKey(userKey: String, warn: Boolean = false): String = {
    userKey
  }


  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }


  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(translateConfKey(key)))
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Get all executor environment variables set on this SparkConf */
  def getExecutorEnv: Seq[(String, String)] = {
    val prefix = "mammuthus.executorEnv."
    getAll.filter {
      case (k, v) => k.startsWith(prefix)
    }
      .map {
      case (k, v) => (k.substring(prefix.length), v)
    }
  }


  /**
   * Returns the Spark application id, valid in the Driver after TaskScheduler registration and
   * from the start in the Executor.
   */
  def getAppId: String = get("mammuthus.app.id")

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.containsKey(translateConfKey(key))

  /** Copy this object */
  override def clone: ApplicationConf = {
    new ApplicationConf(false).setAll(getAll)
  }


  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]) = {
    this.settings.putAll(settings.toMap.asJava)
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): ApplicationConf = {
    settings.putIfAbsent(translateConfKey(key, warn = true), value)
    this
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
   */
  def getenv(name: String): String = System.getenv(name)

  def toDebugString: String = {
    getAll.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }

}
object ApplicationConf {

}
