package mammuthus.yarn


import mammuthus.yarn.util.{IntParam, MemoryParam}

import scala.collection.mutable.ArrayBuffer


/**
 * 8/20/15 WilliamZhu(allwefantasy@gmail.com)
 */
class ApplicationMasterArguments(val args: Array[String]) {
  var userJar: String = null
  var userClass: String = null
  var slaveClass: String = null
  var userArgs: Seq[String] = Seq[String]()
  var executorMemory = 1024
  var executorCores = 1
  var numExecutors = ApplicationMasterArguments.DEFAULT_NUMBER_EXECUTORS

  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    val userArgsBuffer = new ArrayBuffer[String]()

    var args = inputArgs

    while (!args.isEmpty) {
      // --num-workers, --worker-memory, and --worker-cores are deprecated since 1.0,
      // the properties with executor in their names are preferred.
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

        case ("--args" | "--arg") :: value :: tail =>
          userArgsBuffer += value
          args = tail

        case ("--num-workers" | "--num-executors") :: IntParam(value) :: tail =>
          numExecutors = value
          args = tail

        case ("--worker-memory" | "--executor-memory") :: MemoryParam(value) :: tail =>
          executorMemory = value
          args = tail

        case ("--worker-cores" | "--executor-cores") :: IntParam(value) :: tail =>
          executorCores = value
          args = tail

        case _ =>
          printUsageAndExit(1, args)
      }
    }

    userArgs = userArgsBuffer.readOnly
  }

  def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    if (unknownParam != null) {
      System.err.println("Unknown/unsupported param " + unknownParam)
    }
    System.err.println( """
                          |Usage: org.apache.spark.deploy.yarn.ApplicationMaster [options]
                          |Options:
                          |  --jar JAR_PATH       Path to your application's JAR file
                          |  --class CLASS_NAME   Name of your application master's main class
                          |  --slave-class CLASS_NAME   Name of your application slave's main class
                          |  --args ARGS          Arguments to be passed to your application's main class.
                          |                       Multiple invocations are possible, each will be passed in order.
                          |  --num-executors NUM    Number of executors to start (Default: 2)
                          |  --executor-cores NUM   Number of cores for the executors (Default: 1)
                          |  --executor-memory MEM  Memory per executor (e.g. 1000M, 2G) (Default: 1G)
                        """.stripMargin)
    System.exit(exitCode)
  }
}

object ApplicationMasterArguments {
  val DEFAULT_NUMBER_EXECUTORS = 2
}


