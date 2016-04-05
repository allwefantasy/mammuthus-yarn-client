package mammuthus.yarn

import java.net.URL

import scala.collection.mutable


/**
 * 9/1/15 WilliamZhu(allwefantasy@gmail.com)
 */
case class ExecutorBackendParam(driverUrl: String,
                                slaveClass: String,
                                executorId: String,
                                hostname: String,
                                cores: Int,
                                executeMemory: String,
                                appId: String,
                                userClassPath: mutable.ListBuffer[URL]
                                 )

trait ExecutorBackend {
  def parse(args: Array[String]) = {
    var driverUrl: String = null
    var slaveClass: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var executeMemory: String = null
    var appId: String = null
    val userClassPath = new mutable.ListBuffer[URL]()

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--execute-memory") :: value :: tail =>
          executeMemory = value
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--slave-class") :: value :: tail =>
          slaveClass = value
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URL(value)
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          argv = tail

      }
    }
    ExecutorBackendParam(driverUrl, slaveClass, executorId, hostname, cores, executeMemory, appId, userClassPath)
  }
}


