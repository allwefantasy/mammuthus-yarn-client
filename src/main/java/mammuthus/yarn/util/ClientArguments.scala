package mammuthus.yarn.util

import mammuthus.yarn.{ApplicationMasterArguments, ApplicationConf}
import scala.collection.mutable.ArrayBuffer
import mammuthus.yarn.exception.ApplicationException


/**
 * 8/20/15 WilliamZhu(allwefantasy@gmail.com)
 */
class ClientArguments(args: Array[String], applicationConf: ApplicationConf) {
  var addJars: String = null
  var files: String = null
  var archives: String = null
  var userJar: String = null
  var userClass: String = null
  var slaveClass:String  = null
  var keepContainers = true
  var userArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var executorMemory = 1024 // MB
  var executorCores = 1
  var numExecutors = ApplicationMasterArguments.DEFAULT_NUMBER_EXECUTORS
  var amQueue = applicationConf.get("mammuthus.yarn.queue", "default")
  var amMemory: Int = 512 // MB
  var amCores: Int = 1
  var appName: String = "Mammuthus"
  var priority = 0
  def isClusterMode: Boolean = userClass != null

  private var driverMemory: Int = 512 // MB
  private var driverCores: Int = 1
  private val driverMemOverheadKey = "mammuthus.yarn.driver.memoryOverhead"
  private val amMemKey = "mammuthus.yarn.am.memory"
  private val amMemOverheadKey = "mammuthus.yarn.am.memoryOverhead"
  private val driverCoresKey = "mammuthus.driver.cores"
  private val amCoresKey = "mammuthus.yarn.am.cores"
  private val isDynamicAllocationEnabled =
    applicationConf.getBoolean("mammuthus.dynamicAllocation.enabled", false)

  parseArgs(args.toList)
  loadEnvironmentArgs()
  validateArgs()

  // Additional memory to allocate to containers
  val amMemoryOverheadConf = if (isClusterMode) driverMemOverheadKey else amMemOverheadKey
  val amMemoryOverhead = applicationConf.getInt(amMemoryOverheadConf,
    math.max((YarnHadoopUtil.MEMORY_OVERHEAD_FACTOR * amMemory).toInt, YarnHadoopUtil.MEMORY_OVERHEAD_MIN))

  val executorMemoryOverhead = applicationConf.getInt("mammuthus.yarn.executor.memoryOverhead",
    math.max((YarnHadoopUtil.MEMORY_OVERHEAD_FACTOR * executorMemory).toInt, YarnHadoopUtil.MEMORY_OVERHEAD_MIN))

  /** Load any default arguments provided through environment variables and Spark properties. */
  private def loadEnvironmentArgs(): Unit = {
    // For backward compatibility, SPARK_YARN_DIST_{ARCHIVES/FILES} should be resolved to hdfs://,
    // while spark.yarn.dist.{archives/files} should be resolved to file:// (SPARK-2051).
    files = Option(files)
      .orElse(applicationConf.getOption("mammuthus.yarn.dist.files").map(p => Utils.resolveURIs(p)))
      .orElse(sys.env.get("MAMMUTHUS_YARN_DIST_FILES"))
      .orNull
    archives = Option(archives)
      .orElse(applicationConf.getOption("mammuthus.yarn.dist.archives").map(p => Utils.resolveURIs(p)))
      .orElse(sys.env.get("MAMMUTHUS_YARN_DIST_ARCHIVES"))
      .orNull
    // If dynamic allocation is enabled, start at the configured initial number of executors.
    // Default to minExecutors if no initialExecutors is set.
    if (isDynamicAllocationEnabled) {
      val minExecutorsConf = "mammuthus.dynamicAllocation.minExecutors"
      val initialExecutorsConf = "mammuthus.dynamicAllocation.initialExecutors"
      val maxExecutorsConf = "mammuthus.dynamicAllocation.maxExecutors"
      val minNumExecutors = applicationConf.getInt(minExecutorsConf, 0)
      val initialNumExecutors = applicationConf.getInt(initialExecutorsConf, minNumExecutors)
      val maxNumExecutors = applicationConf.getInt(maxExecutorsConf, Integer.MAX_VALUE)

      // If defined, initial executors must be between min and max
      if (initialNumExecutors < minNumExecutors || initialNumExecutors > maxNumExecutors) {
        throw new IllegalArgumentException(
          s"$initialExecutorsConf must be between $minExecutorsConf and $maxNumExecutors!")
      }

      numExecutors = initialNumExecutors
    }
  }

  /**
   * Fail fast if any arguments provided are invalid.
   * This is intended to be called only after the provided arguments have been parsed.
   */
  private def validateArgs(): Unit = {
    if (numExecutors <= 0) {
      throw new IllegalArgumentException(
        "You must specify at least 1 executor!\n" + getUsageMessage())
    }
    if (executorCores < applicationConf.getInt("mammuthus.task.cpus", 1)) {
      throw new ApplicationException("Executor cores must not be less than " +
        "mammuthus.task.cpus.")
    }
    if (isClusterMode) {
      for (key <- Seq(amMemKey, amMemOverheadKey, amCoresKey)) {
        if (applicationConf.contains(key)) {
          println(s"$key is set but does not apply in cluster mode.")
        }
      }
      amMemory = driverMemory
      amCores = driverCores
    } else {
      for (key <- Seq(driverMemOverheadKey, driverCoresKey)) {
        if (applicationConf.contains(key)) {
          println(s"$key is set but does not apply in client mode.")
        }
      }
      applicationConf.getOption(amMemKey)
        .map(Utils.memoryStringToMb)
        .foreach { mem => amMemory = mem }
      applicationConf.getOption(amCoresKey)
        .map(_.toInt)
        .foreach { cores => amCores = cores }
    }
  }

  private def parseArgs(inputArgs: List[String]): Unit = {
    var args = inputArgs

    while (!args.isEmpty) {
      args match {
        case ("--jar") :: value :: tail =>
          userJar = value
          args = tail

        case ("--class") :: value :: tail =>
          userClass = value
          args = tail

        case ("--slave-class") :: value :: tail =>
          slaveClass = value
          args = tail

        case ("--keep-containers") :: value :: tail =>
          keepContainers = value.toBoolean
          args = tail

        case ("--args" | "--arg") :: value :: tail =>
          if (args(0) == "--args") {
            println("--args is deprecated. Use --arg instead.")
          }
          userArgs += value
          args = tail

        case ("--master-class" | "--am-class") :: value :: tail =>
          println(s"${args(0)} is deprecated and is not used anymore.")
          args = tail

        case ("--master-memory" | "--driver-memory") :: MemoryParam(value) :: tail =>
          if (args(0) == "--master-memory") {
            println("--master-memory is deprecated. Use --driver-memory instead.")
          }
          driverMemory = value
          args = tail

        case ("--driver-cores") :: IntParam(value) :: tail =>
          driverCores = value
          args = tail

        case ("--num-workers" | "--num-executors") :: IntParam(value) :: tail =>
          if (args(0) == "--num-workers") {
            println("--num-workers is deprecated. Use --num-executors instead.")
          }
          // Dynamic allocation is not compatible with this option
          if (isDynamicAllocationEnabled) {
            throw new IllegalArgumentException("Explicitly setting the number " +
              "of executors is not compatible with spark.dynamicAllocation.enabled!")
          }
          numExecutors = value
          args = tail

        case ("--worker-memory" | "--executor-memory") :: MemoryParam(value) :: tail =>
          if (args(0) == "--worker-memory") {
            println("--worker-memory is deprecated. Use --executor-memory instead.")
          }
          executorMemory = value
          args = tail

        case ("--worker-cores" | "--executor-cores") :: IntParam(value) :: tail =>
          if (args(0) == "--worker-cores") {
            println("--worker-cores is deprecated. Use --executor-cores instead.")
          }
          executorCores = value
          args = tail

        case ("--queue") :: value :: tail =>
          amQueue = value
          args = tail

        case ("--name") :: value :: tail =>
          appName = value
          args = tail

        case ("--addJars") :: value :: tail =>
          addJars = value
          args = tail


        case ("--files") :: value :: tail =>
          files = value
          args = tail

        case ("--archives") :: value :: tail =>
          archives = value
          args = tail

        case Nil =>

        case _ =>
          throw new IllegalArgumentException(getUsageMessage(args))
      }
    }
  }

  private def getUsageMessage(unknownParam: List[String] = null): String = {
    val message = if (unknownParam != null) s"Unknown/unsupported param $unknownParam\n" else ""
    message +
      """
        |Usage: org.apache.spark.deploy.yarn.Client [options]
        |Options:
        |  --jar JAR_PATH           Path to your application's JAR file (required in yarn-cluster
        |                           mode)
        |  --class CLASS_NAME       Name of your application's main class (required)
        |  --slave-class SLAVE_CLASS_NAME       Name of your application's main class (required)
        |  --arg ARG                Argument to be passed to your application's main class.
        |                           Multiple invocations are possible, each will be passed in order.
        |  --num-executors NUM      Number of executors to start (Default: 2)
        |  --keep-containers        Wheher keep containers when AM failed and retry (Default: true)
        |  --executor-cores NUM     Number of cores per executor (Default: 1).
        |  --driver-memory MEM      Memory for driver (e.g. 1000M, 2G) (Default: 512 Mb)
        |  --driver-cores NUM       Number of cores used by the driver (Default: 1).
        |  --executor-memory MEM    Memory per executor (e.g. 1000M, 2G) (Default: 1G)
        |  --name NAME              The name of your application (Default: Spark)
        |  --queue QUEUE            The hadoop queue to use for allocation requests (Default:
        |                           'default')
        |  --addJars jars           Comma separated list of local jars that want SparkContext.addJar
        |                           to work with.
        |                           place on the PYTHONPATH for Python apps.
        |  --files files            Comma separated list of files to be distributed with the job.
        |  --archives archives      Comma separated list of archives to be distributed with the job.
      """.stripMargin
  }
}
