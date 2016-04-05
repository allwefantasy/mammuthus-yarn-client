package mammuthus.yarn

import java.io.{File, FileWriter, IOException}
import java.lang.reflect.InvocationTargetException
import java.net.URL
import java.util.concurrent.atomic.AtomicInteger

import mammuthus.yarn.log.Logging
import mammuthus.yarn.util.{ChildFirstURLClassLoader, MutableURLClassLoader, Utils, YarnHadoopUtil}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.collection.JavaConversions._
import scala.util.control.NonFatal


/**
 * 8/20/15 WilliamZhu(allwefantasy@gmail.com)
 */
class ApplicationMaster(args: ApplicationMasterArguments, client: YarnRMClient) extends Logging {
  private val conf = new ApplicationConf()
  private val yarnConf: YarnConfiguration = YarnHadoopUtil.get.newConfiguration(conf)
    .asInstanceOf[YarnConfiguration]
  private val isClusterMode = args.userClass != null

  @volatile private var exitCode = 0
  @volatile private var unregistered = false
  @volatile private var finished = false
  @volatile private var finalStatus = getDefaultFinalStatus
  @volatile private var finalMsg: String = ""
  @volatile private var userClassThread: Thread = _
  @volatile private var userApplicationMasterInfo: UserApplicationMasterInfo = _

  private var reporterThread: Thread = _
  private var allocator: YarnAllocator = _

  private val maxNumExecutorFailures = conf.getInt("mammuthus.yarn.max.executor.failures",
    conf.getInt("mammuthus.yarn.max.worker.failures", math.max(args.numExecutors * 2, 3)))

  final def run(): Int = {
    try {
      val appAttemptId = client.getAttemptId()

      logInfo("ApplicationAttemptId: " + appAttemptId)

      val fs = FileSystem.get(yarnConf)
      val cleanupHook = new Runnable {
        override def run() {

          val maxAppAttempts = client.getMaxRegAttempts(conf, yarnConf)
          val isLastAttempt = client.getAttemptId().getAttemptId() >= maxAppAttempts

          if (!finished) {
            // This happens when the user application calls System.exit(). We have the choice
            // of either failing or succeeding at this point. We report success to avoid
            // retrying applications that have succeeded (System.exit(0)), which means that
            // applications that explicitly exit with a non-zero status will also show up as
            // succeeded in the RM UI.
            finish(finalStatus,
              ApplicationMaster.EXIT_SUCCESS,
              "Shutdown hook called before final status was reported.")
          }

          if (!unregistered) {
            // we only want to unregister if we don't want the RM to retry
            if (finalStatus == FinalApplicationStatus.SUCCEEDED || isLastAttempt) {
              unregister(finalStatus, finalMsg)
              cleanupStagingDir(fs)
            }
          }
        }
      }

      // Use higher priority than FileSystem.
      assert(ApplicationMaster.SHUTDOWN_HOOK_PRIORITY > FileSystem.SHUTDOWN_HOOK_PRIORITY)
      ShutdownHookManager
        .get().addShutdownHook(cleanupHook, ApplicationMaster.SHUTDOWN_HOOK_PRIORITY)

      runDriver()
    } catch {
      case e: Exception =>
        // catch everything else if not specifically handled
        logError("Uncaught exception: ", e)
        finish(FinalApplicationStatus.FAILED,
          ApplicationMaster.EXIT_UNCAUGHT_EXCEPTION,
          "Uncaught exception: " + e.getMessage())
    }
    exitCode
  }

  /** Add the Yarn IP filter that is required for properly securing the UI. */
  private def addAmIpFilter() = {
    val proxyBase = System.getenv(ApplicationConstants.APPLICATION_WEB_PROXY_BASE_ENV)
    val amFilter = "org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter"
    val params = client.getAmIpFilterParams(yarnConf, proxyBase)
    if (isClusterMode) {
      System.setProperty("mammuthus.ui.filters", amFilter)
      params.foreach {
        case (k, v) => System.setProperty(s"mammuthus.$amFilter.param.$k", v)
      }
    }
  }

  private def runDriver(): Unit = {
    addAmIpFilter()
    val params = new java.util.HashMap[String, String]()
    userClassThread = startUserApplication(params)
    registerAM(params)
    userClassThread.join()
  }

  /**
   * Clean up the staging directory.
   */
  private def cleanupStagingDir(fs: FileSystem) {
    var stagingDirPath: Path = null
    try {
      val preserveFiles = conf.getBoolean("mammuthus.yarn.preserve.staging.files", false)
      if (!preserveFiles) {
        stagingDirPath = new Path(System.getenv("MAMMUTHUS_YARN_STAGING_DIR"))
        if (stagingDirPath == null) {
          logError("Staging directory is null")
          return
        }
        logInfo("Deleting staging directory " + stagingDirPath)
        fs.delete(stagingDirPath, true)
      }
    } catch {
      case ioe: IOException =>
        logError("Failed to cleanup staging dir " + stagingDirPath, ioe)
    }
  }

  /**
   * Set the default final application status for client mode to UNDEFINED to handle
   * if YARN HA restarts the application so that it properly retries. Set the final
   * status to SUCCEEDED in cluster mode to handle if the user calls System.exit
   * from the application code.
   */
  final def getDefaultFinalStatus() = {
    if (isClusterMode) {
      FinalApplicationStatus.SUCCEEDED
    } else {
      FinalApplicationStatus.UNDEFINED
    }
  }

  /**
   * unregister is used to completely unregister the application from the ResourceManager.
   * This means the ResourceManager will not retry the application attempt on your behalf if
   * a failure occurred.
   */
  final def unregister(status: FinalApplicationStatus, diagnostics: String = null) = synchronized {
    if (!unregistered) {
      logInfo(s"Unregistering ApplicationMaster with $status" +
        Option(diagnostics).map(msg => s" (diag message: $msg)").getOrElse(""))
      unregistered = true
      client.unregister(status, Option(diagnostics).getOrElse(""))
    }
  }

  final def finish(status: FinalApplicationStatus, code: Int, msg: String = null) = synchronized {
    if (!finished) {
      val inShutdown = Utils.inShutdown()
      logInfo(s"Final app status: ${status}, exitCode: ${code}" +
        Option(msg).map(msg => s", (reason: $msg)").getOrElse(""))
      exitCode = code
      finalStatus = status
      finalMsg = msg
      finished = true
      if (!inShutdown && Thread.currentThread() != reporterThread && reporterThread != null) {
        logDebug("shutting down reporter thread")
        reporterThread.interrupt()
      }
      if (!inShutdown && Thread.currentThread() != userClassThread && userClassThread != null) {
        logDebug("shutting down user thread")
        userClassThread.interrupt()
      }
    }
  }

  private def registerAM(params: java.util.Map[String, String]) = {
    val counter = new AtomicInteger(0)
    while (params.isEmpty && counter.get() < 30) {
      Thread.sleep(1000)
      counter.addAndGet(1)
    }
    params.foreach(f => s"registerAM=> (${f._1} -> ${f._2})")
    userApplicationMasterInfo = UserApplicationMasterInfo(Utils.localHostName(), params("httpPort").toInt, params("uiAddress"))
    conf.set("mammuthus.user.app.host", userApplicationMasterInfo.host)
    conf.set("mammuthus.user.app.port", userApplicationMasterInfo.port + "")
    allocator = client.register(yarnConf,
      conf,
      userApplicationMasterInfo.uiAddress,
      "")
    allocator.allocateResources()
    reporterThread = launchReporterThread()
  }

  private def launchReporterThread(): Thread = {
    // Ensure that progress is sent before YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS elapses.
    val expiryInterval = yarnConf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS, 120000)

    // we want to be reasonably responsive without causing too many requests to RM.
    val schedulerInterval =
      conf.getLong("mammuthus.yarn.scheduler.heartbeat.interval-ms", 5000)

    // must be <= expiryInterval / 2.
    val interval = math.max(0, math.min(expiryInterval / 2, schedulerInterval))

    // The number of failures in a row until Reporter thread give up
    val reporterMaxFailures = conf.getInt("mammuthus.yarn.scheduler.reporterThread.maxFailures", 5)

    val t = new Thread {
      override def run() {
        var failureCount = 0
        while (!finished) {
          try {
            logInfo("Reporter req")
            if (allocator.getNumExecutorsFailed >= maxNumExecutorFailures) {
              finish(FinalApplicationStatus.FAILED,
                ApplicationMaster.EXIT_MAX_EXECUTOR_FAILURES,
                "Max number of executor failures reached")
            } else {
              logInfo("Sending container request progress")
              allocator.allocateResources()
            }
            failureCount = 0
          } catch {
            case i: InterruptedException =>
            case e: Throwable => {
              failureCount += 1
              if (!NonFatal(e) || failureCount >= reporterMaxFailures) {
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_REPORTER_FAILURE, "Exception was thrown " +
                    s"${failureCount} time(s) from Reporter thread.")

              } else {
                logWarning(s"Reporter thread fails ${failureCount} time(s) in a row.", e)
              }
            }
          }
          try {
            Thread.sleep(interval)
          } catch {
            case e: InterruptedException =>
          }
        }
      }
    }
    // setting to daemon status, though this is usually not a good idea.
    t.setDaemon(true)
    t.setName("Reporter")
    t.start()
    logInfo("Started progress reporter thread - sleep time : " + interval)
    t
  }

  case class UserApplicationMasterInfo(host: String, port: Int, uiAddress: String)

  private def internalStartUserApplication(userClzz: Class[_], params: java.util.Map[String, String]) = {
    val mainArgs = new Array[String](args.userArgs.size)
    args.userArgs.copyToArray(mainArgs, 0, args.userArgs.size)

    if (!classOf[MammuthusApplication].isAssignableFrom(userClzz)) {
      val mainMethod = userClzz.getMethod("main", classOf[Array[String]])
      mainMethod.invoke(null, mainArgs)
    } else {
      val application = userClzz.newInstance().asInstanceOf[MammuthusApplication]
      application.run(mainArgs, params)
    }
  }

  def execWithUserAndExitValue(user: String, shellStr: String, timeout: Long) = {
    def using[A <: {def close() : Unit}, B](param: A)(f: A => B): B =
      try {
        f(param)
      } finally {
        param.close()
      }

    def writeToFile(fileName: String, data: String) =
      using(new FileWriter(fileName)) {
        fileWriter => fileWriter.append(data)
      }

    def wrapCommand(user: String, fileName: String) = {
      if (user != null && !user.isEmpty) {
        s"su - $user /bin/bash -c '/bin/bash /tmp/$fileName'"
      } else s"/bin/bash /tmp/$fileName"
    }
    import scala.sys.process._
    val out = new StringBuilder
    val err = new StringBuilder
    val et = ProcessLogger(
      line => out.append(line + "\n"),
      line => err.append(line + "\n"))

    val fileName = System.currentTimeMillis() + "_" + Math.random() + ".sh"
    writeToFile("/tmp/" + fileName, "#!/bin/bash\n" + shellStr)
    s"chmod u+x /tmp/$fileName".!
    val pb = Process(wrapCommand(user, fileName))
    val exitValue = pb ! et
    s"rm /tmp/$fileName".!
    (exitValue, err.toString().trim, out.toString().trim)
  }

  private def startUserApplication(params: java.util.Map[String, String]): Thread = {
    logInfo("Starting the user application in a separate Thread")
    System.setProperty("mammuthus.executor.instances", args.numExecutors.toString)

    val classpath = Client.getUserClasspath(conf)

    val urls = classpath.map {
      entry =>
        new URL("file:" + new File(entry.getPath()).getAbsolutePath())
    }
    val userClassLoader =
      if (Client.isUserClassPathFirst(conf, isDriver = true)) {
        new ChildFirstURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      } else {
        new MutableURLClassLoader(urls, Utils.getContextOrSparkClassLoader)
      }

    val userThread = new Thread {
      override def run() {
        try {
          internalStartUserApplication(userClassLoader.loadClass(args.userClass), params)
          finish(FinalApplicationStatus.SUCCEEDED, ApplicationMaster.EXIT_SUCCESS)
          logInfo("Done running users class")
        } catch {
          case e: InvocationTargetException =>
            e.getCause match {
              case _: InterruptedException =>
              // Reporter thread can interrupt to stop user class
              case cause: Throwable =>
                finish(FinalApplicationStatus.FAILED,
                  ApplicationMaster.EXIT_EXCEPTION_USER_CLASS,
                  "User class threw exception: " + cause.getMessage)
                // re-throw to get it logged
                throw cause
            }
        }
      }
    }
    userThread.setContextClassLoader(userClassLoader)
    userThread.setName("Driver")
    userThread.start()
    userThread
  }

}

object ApplicationMaster extends Logging {

  val SHUTDOWN_HOOK_PRIORITY: Int = 30

  // exit codes for different causes, no reason behind the values
  private val EXIT_SUCCESS = 0
  private val EXIT_UNCAUGHT_EXCEPTION = 10
  private val EXIT_MAX_EXECUTOR_FAILURES = 11
  private val EXIT_REPORTER_FAILURE = 12
  private val EXIT_SC_NOT_INITED = 13
  private val EXIT_SECURITY = 14
  private val EXIT_EXCEPTION_USER_CLASS = 15

  private var master: ApplicationMaster = _

  def main(args: Array[String]) = {
    val amArgs = new ApplicationMasterArguments(args)
    YarnHadoopUtil.get.runAsSparkUser {
      () =>
        master = new ApplicationMaster(amArgs, new YarnRMClient(amArgs))
        System.exit(master.run())
    }
  }
}
