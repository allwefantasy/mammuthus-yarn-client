package mammuthus.yarn


import java.net.{InetAddress, URI, UnknownHostException}
import java.nio.ByteBuffer

import com.google.common.base.Objects
import mammuthus.yarn.cache.ClientDistributedCacheManager
import mammuthus.yarn.exception.ApplicationException
import mammuthus.yarn.log.Logging
import mammuthus.yarn.util.{ClientArguments, Utils, YarnHadoopUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClient, YarnClientApplication}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.Records

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer, Map}
import scala.util.{Failure, Success, Try}

/**
 * 8/20/15 WilliamZhu(allwefantasy@gmail.com)
 */
class Client(val args: ClientArguments,
             val hadoopConf: Configuration,
             val applicationConf: ApplicationConf)
  extends Logging {

  import Client._

  def this(clientArgs: ClientArguments, spConf: ApplicationConf) =
    this(clientArgs, YarnHadoopUtil.get.newConfiguration(spConf), spConf)

  def this(clientArgs: ClientArguments) = this(clientArgs, new ApplicationConf())

  def run(): Unit = {
    val (yarnApplicationState, finalApplicationStatus) = monitorApplication(submitApplication())
    if (yarnApplicationState == YarnApplicationState.FAILED ||
      finalApplicationStatus == FinalApplicationStatus.FAILED) {
      throw new ApplicationException("Application finished with failed status")
    }
    if (yarnApplicationState == YarnApplicationState.KILLED ||
      finalApplicationStatus == FinalApplicationStatus.KILLED) {
      throw new ApplicationException("Application is killed")
    }
    if (finalApplicationStatus == FinalApplicationStatus.UNDEFINED) {
      throw new ApplicationException("The final status of application is undefined")
    }
  }

  private val yarnClient = YarnClient.createYarnClient
  private val yarnConf = new YarnConfiguration(hadoopConf)
  private val credentials = UserGroupInformation.getCurrentUser.getCredentials
  private val amMemoryOverhead = args.amMemoryOverhead
  // MB
  private val executorMemoryOverhead = args.executorMemoryOverhead
  // MB
  private val distCacheMgr = new ClientDistributedCacheManager()
  private val isClusterMode = args.isClusterMode


  def stop(): Unit = yarnClient.stop()

  /* ------------------------------------------------------------------------------------- *
   | The following methods have much in common in the stable and alpha versions of Client, |
   | but cannot be implemented in the parent trait due to subtle API differences across    |
   | hadoop versions.                                                                      |
   * ------------------------------------------------------------------------------------- */

  /**
   * Submit an application running our ApplicationMaster to the ResourceManager.
   *
   * The stable Yarn API provides a convenience method (YarnClient#createApplication) for
   * creating applications and setting up the application submission context. This was not
   * available in the alpha API.
   */
  def submitApplication(): ApplicationId = {
    yarnClient.init(yarnConf)
    yarnClient.start()

    logInfo("Requesting a new application from cluster with %d NodeManagers"
      .format(yarnClient.getYarnClusterMetrics.getNumNodeManagers))

    // Get a new application from our RM
    val newApp = yarnClient.createApplication()
    val newAppResponse = newApp.getNewApplicationResponse()
    val appId = newAppResponse.getApplicationId()

    // Verify whether the cluster has enough resources for our AM
    verifyClusterResources(newAppResponse)

    // Set up the appropriate contexts to launch our AM
    val containerContext = createContainerLaunchContext(newAppResponse)
    val appContext = createApplicationSubmissionContext(newApp, containerContext)

    // Finally, submit and monitor the application
    logInfo(s"Submitting application ${appId.getId} to ResourceManager")
    yarnClient.submitApplication(appContext)
    appId
  }

  /**
   * Set up the context for submitting our ApplicationMaster.
   * This uses the YarnClientApplication not available in the Yarn alpha API.
   */
  def createApplicationSubmissionContext(
                                          newApp: YarnClientApplication,
                                          containerContext: ContainerLaunchContext): ApplicationSubmissionContext = {
    val appContext = newApp.getApplicationSubmissionContext
    appContext.setApplicationName(args.appName)
    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(containerContext)
    if (args.keepContainers) {
      appContext.setKeepContainersAcrossApplicationAttempts(true)
    }
    appContext.setApplicationType("MAMMUTHUS")
    applicationConf.getOption("mammuthus.yarn.maxAppAttempts").map(_.toInt) match {
      case Some(v) => appContext.setMaxAppAttempts(v)
      case None => logDebug("mammuthus.yarn.maxAppAttempts is not set. " +
        "Cluster's default value will be used.")
    }
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(args.amMemory + amMemoryOverhead)
    capability.setVirtualCores(args.amCores)
    appContext.setResource(capability)
    appContext
  }

  /** Set up security tokens for launching our ApplicationMaster container. */
  private def setupSecurityToken(amContainer: ContainerLaunchContext): Unit = {
    val dob = new DataOutputBuffer
    credentials.writeTokenStorageToStream(dob)
    amContainer.setTokens(ByteBuffer.wrap(dob.getData))
  }

  /** Get the application report from the ResourceManager for an application we have submitted. */
  def getApplicationReport(appId: ApplicationId): ApplicationReport =
    yarnClient.getApplicationReport(appId)

  /**
   * Return the security token used by this client to communicate with the ApplicationMaster.
   * If no security is enabled, the token returned by the report is null.
   */
  private def getClientToken(report: ApplicationReport): String =
    Option(report.getClientToAMToken).map(_.toString).getOrElse("")

  /**
   * Fail fast if we have requested more resources per container than is available in the cluster.
   */
  private def verifyClusterResources(newAppResponse: GetNewApplicationResponse): Unit = {
    val maxMem = newAppResponse.getMaximumResourceCapability().getMemory()
    logInfo("Verifying our application has not requested more than the maximum " +
      s"memory capability of the cluster ($maxMem MB per container)")
    val executorMem = args.executorMemory + executorMemoryOverhead
    if (executorMem > maxMem) {
      throw new IllegalArgumentException(s"Required executor memory (${args.executorMemory}" +
        s"+$executorMemoryOverhead MB) is above the max threshold ($maxMem MB) of this cluster!")
    }
    val amMem = args.amMemory + amMemoryOverhead
    if (amMem > maxMem) {
      throw new IllegalArgumentException(s"Required AM memory (${args.amMemory}" +
        s"+$amMemoryOverhead MB) is above the max threshold ($maxMem MB) of this cluster!")
    }
    logInfo("Will allocate AM container, with %d MB memory including %d MB overhead".format(
      amMem,
      amMemoryOverhead))

    // We could add checks to make sure the entire cluster has enough resources but that involves
    // getting all the node reports and computing ourselves.
  }

  /**
   * Copy the given file to a remote file system (e.g. HDFS) if needed.
   * The file is only copied if the source and destination file systems are different. This is used
   * for preparing resources for launching the ApplicationMaster container. Exposed for testing.
   */
  private def copyFileToRemote(
                                destDir: Path,
                                srcPath: Path,
                                replication: Short): Path = {
    val destFs = destDir.getFileSystem(hadoopConf)
    val srcFs = srcPath.getFileSystem(hadoopConf)
    var destPath = srcPath
    if (!compareFs(srcFs, destFs)) {
      destPath = new Path(destDir, srcPath.getName())
      logInfo(s"Uploading resource $srcPath -> $destPath")
      FileUtil.copy(srcFs, srcPath, destFs, destPath, false, hadoopConf)
      destFs.setReplication(destPath, replication)
      destFs.setPermission(destPath, new FsPermission(APP_FILE_PERMISSION))
    } else {
      logInfo(s"Source and destination file systems are the same. Not copying $srcPath")
    }
    // Resolve any symlinks in the URI path so using a "current" symlink to point to a specific
    // version shows the specific version in the distributed cache configuration
    val qualifiedDestPath = destFs.makeQualified(destPath)
    val fc = FileContext.getFileContext(qualifiedDestPath.toUri(), hadoopConf)
    fc.resolvePath(qualifiedDestPath)
  }

  private def getNameNodesToAccess(sparkConf: ApplicationConf): Set[Path] = {
    sparkConf.get("mammuthus.yarn.access.namenodes", "")
      .split(",")
      .map(_.trim())
      .filter(!_.isEmpty)
      .map(new Path(_))
      .toSet
  }

  private def obtainTokensForNamenodes(
                                        paths: Set[Path],
                                        conf: Configuration,
                                        creds: Credentials): Unit = {

  }

  /**
   * Upload any resources to the distributed cache if needed. If a resource is intended to be
   * consumed locally, set up the appropriate config for downstream code to handle it properly.
   * This is used for setting up a container launch context for our ApplicationMaster.
   * Exposed for testing.
   */
  def prepareLocalResources(appStagingDir: String): HashMap[String, LocalResource] = {
    logInfo("Preparing resources for our AM container")
    // Upload Spark and the application JAR to the remote file system if necessary,
    // and add them as local resources to the application master.
    val fs = FileSystem.get(hadoopConf)
    val dst = new Path(fs.getHomeDirectory(), appStagingDir)
    val nns = getNameNodesToAccess(applicationConf) + dst
    obtainTokensForNamenodes(nns, hadoopConf, credentials)

    val replication = applicationConf.getInt("mammuthus.yarn.submit.file.replication",
      fs.getDefaultReplication(dst)).toShort
    val localResources = HashMap[String, LocalResource]()
    FileSystem.mkdirs(fs, dst, new FsPermission(STAGING_DIR_PERMISSION))

    val statCache: Map[URI, FileStatus] = HashMap[URI, FileStatus]()

    val oldLog4jConf = Option(System.getenv("SPARK_LOG4J_CONF"))
    if (oldLog4jConf.isDefined) {
      logWarning(
        "SPARK_LOG4J_CONF detected in the system environment. This variable has been " +
          "deprecated. Please refer to the \"Launching Spark on YARN\" documentation " +
          "for alternatives.")
    }

    /**
     * Copy the given main resource to the distributed cache if the scheme is not "local".
     * Otherwise, set the corresponding key in our SparkConf to handle it downstream.
     * Each resource is represented by a 3-tuple of:
     * (1) destination resource name,
     * (2) local path to the resource,
     * (3) Spark property key to set if the scheme is not local
     */
    List(
      (MAMMUTHUS_YARN_CLIENT_JAR, mammuthusJar(applicationConf), CONF_MAMMUTHUS_YARN_CLIENT__JAR),
      (APP_JAR, args.userJar, CONF_MAMMUTHUS_YARN_CLIENT_USER_JAR),
      ("log4j.properties", oldLog4jConf.orNull, null)
    ).foreach {
      case (destName, _localPath, confKey) =>
        val localPath: String = if (_localPath != null) _localPath.trim() else ""
        if (!localPath.isEmpty()) {
          val localURI = new URI(localPath)
          if (localURI.getScheme != LOCAL_SCHEME) {
            val src = getQualifiedLocalPath(localURI, hadoopConf)
            val destPath = copyFileToRemote(dst, src, replication)
            val destFs = FileSystem.get(destPath.toUri(), hadoopConf)
            distCacheMgr.addResource(destFs, hadoopConf, destPath,
              localResources, LocalResourceType.FILE, destName, statCache)
          } else if (confKey != null) {
            // If the resource is intended for local use only, handle this downstream
            // by setting the appropriate property
            applicationConf.set(confKey, localPath)
          }
        }
    }

    /**
     * Do the same for any additional resources passed in through ClientArguments.
     * Each resource category is represented by a 3-tuple of:
     * (1) comma separated list of resources in this category,
     * (2) resource type, and
     * (3) whether to add these resources to the classpath
     */
    val cachedSecondaryJarLinks = ListBuffer.empty[String]
    List(
      (args.addJars, LocalResourceType.FILE, true),
      (args.files, LocalResourceType.FILE, false),
      (args.archives, LocalResourceType.ARCHIVE, false)
    ).foreach {
      case (flist, resType, addToClasspath) =>
        if (flist != null && !flist.isEmpty()) {
          flist.split(',').foreach {
            file =>
              val localURI = new URI(file.trim())
              if (localURI.getScheme != LOCAL_SCHEME) {
                val localPath = new Path(localURI)
                val linkname = Option(localURI.getFragment()).getOrElse(localPath.getName())
                val destPath = copyFileToRemote(dst, localPath, replication)
                distCacheMgr.addResource(
                  fs, hadoopConf, destPath, localResources, resType, linkname, statCache)
                if (addToClasspath) {
                  cachedSecondaryJarLinks += linkname
                }
              } else if (addToClasspath) {
                // Resource is intended for local use only and should be added to the class path
                cachedSecondaryJarLinks += file.trim()
              }
          }
        }
    }
    if (cachedSecondaryJarLinks.nonEmpty) {
      applicationConf.set(CONF_MAMMUTHUS_YARN_CLIENT_YARN_SECONDARY_JARS, cachedSecondaryJarLinks.mkString(","))
    }

    localResources
  }

  /**
   * Set up the environment for launching our ApplicationMaster container.
   */
  private def setupLaunchEnv(stagingDir: String): HashMap[String, String] = {
    logInfo("Setting up the launch environment for our AM container")
    val env = new HashMap[String, String]()
    val extraCp = applicationConf.getOption("mammuthus.driver.extraClassPath")
    populateClasspath(args, yarnConf, applicationConf, env, extraCp)
    env("MAMMUTHUS_YARN_MODE") = "true"
    env("MAMMUTHUS_YARN_STAGING_DIR") = stagingDir
    env("MAMMUTHUS_USER") = UserGroupInformation.getCurrentUser().getShortUserName()
    log.info("setupLaunchEnv am env " + env)
    // Set the environment variables to be passed on to the executors.
    distCacheMgr.setDistFilesEnv(env)
    distCacheMgr.setDistArchivesEnv(env)

    // Pick up any environment variables for the AM provided through spark.yarn.appMasterEnv.*
    val amEnvPrefix = "mammuthus.yarn.appMasterEnv."
    applicationConf.getAll
      .filter {
      case (k, v) => k.startsWith(amEnvPrefix)
    }
      .map {
      case (k, v) => (k.substring(amEnvPrefix.length), v)
    }
      .foreach {
      case (k, v) => YarnHadoopUtil.addPathToEnvironment(env, k, v)
    }

    // Keep this for backwards compatibility but users should move to the config
    sys.env.get("MAMMUTHUS_YARN_USER_ENV").foreach {
      userEnvs =>
        // Allow users to specify some environment variables.
        YarnHadoopUtil.setEnvFromInputString(env, userEnvs)
        // Pass SPARK_YARN_USER_ENV itself to the AM so it can use it to set up executor environments.
        env("MAMMUTHUS_YARN_USER_ENV") = userEnvs
    }

    // In cluster mode, if the deprecated SPARK_JAVA_OPTS is set, we need to propagate it to
    // executors. But we can't just set spark.executor.extraJavaOptions, because the driver's
    // SparkContext will not let that set spark* system properties, which is expected behavior for
    // Yarn clients. So propagate it through the environment.
    //
    // Note that to warn the user about the deprecation in cluster mode, some code from
    // SparkConf#validateSettings() is duplicated here (to avoid triggering the condition
    // described above).

    sys.env.get(ENV_DIST_CLASSPATH).foreach {
      dcp =>
        env(ENV_DIST_CLASSPATH) = dcp
    }

    env
  }

  private def getAppStagingDir(appId: ApplicationId): String = {
    buildPath(MAMMUTHUS_YARN_CLIENT_STAGING, appId.toString())
  }

  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

  /**
   * Set up a ContainerLaunchContext to launch our ApplicationMaster container.
   * This sets up the launch environment, java options, and the command for launching the AM.
   */
  private def createContainerLaunchContext(newAppResponse: GetNewApplicationResponse): ContainerLaunchContext = {
    logInfo("Setting up container launch context for our AM")

    val appId = newAppResponse.getApplicationId
    val appStagingDir = getAppStagingDir(appId)
    val localResources = prepareLocalResources(appStagingDir)
    val launchEnv = setupLaunchEnv(appStagingDir)
    val amContainer = Records.newRecord(classOf[ContainerLaunchContext])
    amContainer.setLocalResources(localResources)
    amContainer.setEnvironment(launchEnv)
    log.info("amContainer setEnvironment " + launchEnv)
    val javaOpts = ListBuffer[String]()

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    var prefixEnv: Option[String] = None

    // Add Xmx for AM memory
    javaOpts += "-Xmx" + args.amMemory + "m"

    val tmpDir = new Path(
      YarnHadoopUtil.expandEnvironment(Environment.PWD),
      YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR
    )
    javaOpts += "-Djava.io.tmpdir=" + tmpDir

    // TODO: Remove once cpuset version is pushed out.
    // The context is, default gc for server class machines ends up using all cores to do gc -
    // hence if there are multiple containers in same node, Spark GC affects all other containers'
    // performance (which can be that of other Spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce "proper" Spark behavior in
    // multi-tenant environments. Not sure how default Java GC behaves if it is limited to subset
    // of cores on a node.
    val useConcurrentAndIncrementalGC = launchEnv.get("MAMMUTHUS_USE_CONC_INCR_GC").exists(_.toBoolean)
    if (useConcurrentAndIncrementalGC) {
      // In our expts, using (default) throughput collector has severe perf ramifications in
      // multi-tenant machines
      javaOpts += "-XX:+UseConcMarkSweepGC"
      javaOpts += "-XX:MaxTenuringThreshold=31"
      javaOpts += "-XX:SurvivorRatio=8"
      javaOpts += "-XX:+CMSIncrementalMode"
      javaOpts += "-XX:+CMSIncrementalPacing"
      javaOpts += "-XX:CMSIncrementalDutyCycleMin=0"
      javaOpts += "-XX:CMSIncrementalDutyCycle=10"
    }

    // Forward the Spark configuration to the application master / executors.
    // TODO: it might be nicer to pass these as an internal environment variable rather than
    // as Java options, due to complications with string parsing of nested quotes.
    for ((k, v) <- applicationConf.getAll) {
      javaOpts += YarnHadoopUtil.escapeForShell(s"-D$k=$v")
    }

    // Include driver-specific java options if we are launching a driver
    if (isClusterMode) {
      val driverOpts = applicationConf.getOption("mammuthus.driver.extraJavaOptions")
        .orElse(sys.env.get("MAMMUTHUS_JAVA_OPTS"))
      driverOpts.foreach {
        opts =>
          javaOpts ++= Utils.splitCommandString(opts).map(YarnHadoopUtil.escapeForShell)
      }
      val libraryPaths = Seq(sys.props.get("mammuthus.driver.extraLibraryPath"),
        sys.props.get("mammuthus.driver.libraryPath")).flatten
      if (libraryPaths.nonEmpty) {
        prefixEnv = Some(Utils.libraryPathEnvPrefix(libraryPaths))
      }
      if (applicationConf.getOption("mammuthus.yarn.am.extraJavaOptions").isDefined) {
        logWarning("mammuthus.yarn.am.extraJavaOptions will not take effect in cluster mode")
      }
    } else {
      // Validate and include yarn am specific java options in yarn-client mode.
      val amOptsKey = "mammuthus.yarn.am.extraJavaOptions"
      val amOpts = applicationConf.getOption(amOptsKey)
      amOpts.foreach {
        opts =>
          if (opts.contains("-Dmammuthus")) {
            val msg = s"$amOptsKey is not allowed to set Spark options (was '$opts'). "
            throw new ApplicationException(msg)
          }
          if (opts.contains("-Xmx") || opts.contains("-Xms")) {
            val msg = s"$amOptsKey is not allowed to alter memory settings (was '$opts')."
            throw new ApplicationException(msg)
          }
          javaOpts ++= Utils.splitCommandString(opts).map(YarnHadoopUtil.escapeForShell)
      }
    }

    // For log4j configuration to reference
    javaOpts += ("-Dmammuthus.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)

    val userClass =
      if (isClusterMode) {
        Seq("--class", YarnHadoopUtil.escapeForShell(args.userClass))
      } else {
        Nil
      }

    val slaveClass =
      if (isClusterMode) {
        Seq("--slave-class", YarnHadoopUtil.escapeForShell(args.slaveClass))
      } else {
        Nil
      }

    val userJar =
      if (args.userJar != null) {
        Seq("--jar", args.userJar)
      } else {
        Nil
      }
    val amClass = Class.forName("mammuthus.yarn.ApplicationMaster").getName

    val userArgs = args.userArgs.flatMap {
      arg =>
        Seq("--arg", YarnHadoopUtil.escapeForShell(arg))
    }
    val amArgs =
      Seq(amClass) ++ userClass ++ slaveClass ++ userJar ++ userArgs ++
        Seq(
          "--executor-memory", args.executorMemory.toString + "m",
          "--executor-cores", args.executorCores.toString,
          "--num-executors ", args.numExecutors.toString)


    // Command for the ApplicationMaster
    val commands = prefixEnv ++ Seq(
      YarnHadoopUtil.expandEnvironment(Environment.JAVA_HOME) + "/bin/java", "-server"
    ) ++
      javaOpts ++ amArgs ++
      Seq(
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    val printableCommands = commands.map(s => if (s == null) "null" else s).toList
    amContainer.setCommands(printableCommands)

    logInfo("===============================================================================")
    logInfo("Yarn AM launch context:")
    logInfo(s"    user class: ${Option(args.userClass).getOrElse("N/A")}")
    logInfo("    env:")
    launchEnv.foreach {
      case (k, v) => logInfo(s"        $k -> $v")
    }
    logInfo("    resources:")
    localResources.foreach {
      case (k, v) => logDebug(s"        $k -> $v")
    }
    logInfo("    command:")
    logInfo(s"        ${printableCommands.mkString(" ")}")
    logInfo("===============================================================================")

    amContainer
  }

  /**
   * Report the state of an application until it has exited, either successfully or
   * due to some failure, then return a pair of the yarn application state (FINISHED, FAILED,
   * KILLED, or RUNNING) and the final application state (UNDEFINED, SUCCEEDED, FAILED,
   * or KILLED).
   *
   * @param appId ID of the application to monitor.
   * @param returnOnRunning Whether to also return the application state when it is RUNNING.
   * @param logApplicationReport Whether to log details of the application report every iteration.
   * @return A pair of the yarn application state and the final application state.
   */
  def monitorApplication(
                          appId: ApplicationId,
                          returnOnRunning: Boolean = true,
                          logApplicationReport: Boolean = true): (YarnApplicationState, FinalApplicationStatus) = {
    val interval = applicationConf.getLong("spark.yarn.report.interval", 1000)
    var lastState: YarnApplicationState = null

    while (true) {
      Thread.sleep(interval)
      val report = getApplicationReport(appId)
      val state = report.getYarnApplicationState

      if (logApplicationReport) {
        logInfo(s"Application report for $appId (state: $state)")
        val details = Seq[(String, String)](
          ("client token", getClientToken(report)),
          ("diagnostics", report.getDiagnostics),
          ("ApplicationMaster host", report.getHost),
          ("ApplicationMaster RPC port", report.getRpcPort.toString),
          ("queue", report.getQueue),
          ("start time", report.getStartTime.toString),
          ("final status", report.getFinalApplicationStatus.toString),
          ("tracking URL", report.getTrackingUrl),
          ("user", report.getUser)
        )

        // Use more loggable format if value is null or empty
        val formattedDetails = details
          .map {
          case (k, v) =>
            val newValue = Option(v).filter(_.nonEmpty).getOrElse("N/A")
            s"\n\t $k: $newValue"
        }
          .mkString("")

        // If DEBUG is enabled, log report details every iteration
        // Otherwise, log them every time the application changes state
        if (log.isDebugEnabled) {
          logDebug(formattedDetails)
        } else if (lastState != state) {
          logInfo(formattedDetails)
        }
      }

      if (state == YarnApplicationState.FINISHED ||
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
        return (state, report.getFinalApplicationStatus)
      }

      if (returnOnRunning && state == YarnApplicationState.RUNNING) {
        return (state, report.getFinalApplicationStatus)
      }

      lastState = state
    }

    // Never reached, but keeps compiler happy
    throw new ApplicationException("While loop is depleted! This should never happen...")
  }
}

object Client extends Logging {
  def main(argStrings: Array[String]) {

    val applicationConf = new ApplicationConf()

    val args = new ClientArguments(argStrings, applicationConf)
    new Client(args, applicationConf).run()
  }

  // Alias for the Spark assembly jar and the user jar
  val MAMMUTHUS_YARN_CLIENT_JAR: String = "__mammuthus_yarn_client__.jar"
  val APP_JAR: String = "__app__.jar"

  // URI scheme that identifies local resources
  val LOCAL_SCHEME = "local"

  // Staging directory for any temporary jars or files
  val MAMMUTHUS_YARN_CLIENT_STAGING: String = ".mammuthusYarnClientStaging"

  // Location of any user-defined Spark jars
  val CONF_MAMMUTHUS_YARN_CLIENT__JAR = "mammuthus.yarn.client.yarn.jar"
  val ENV_MAMMUTHUS_YARN_CLIENT__JAR = "MAMMUTHUS_YARN_CLIENT_JAR"

  // Internal config to propagate the location of the user's jar to the driver/executors
  val CONF_MAMMUTHUS_YARN_CLIENT_USER_JAR = "mammuthus.yarn.user.jar"

  // Internal config to propagate the locations of any extra jars to add to the classpath
  // of the executors
  val CONF_MAMMUTHUS_YARN_CLIENT_YARN_SECONDARY_JARS = "mammuthus.yarn.client.yarn.secondary.jars"

  // Staging directory is private! -> rwx--------
  val STAGING_DIR_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("700", 8).toShort)

  // App files are world-wide readable and owner writable -> rw-r--r--
  val APP_FILE_PERMISSION: FsPermission =
    FsPermission.createImmutable(Integer.parseInt("644", 8).toShort)

  // Distribution-defined classpath to add to processes
  val ENV_DIST_CLASSPATH = "MAMMUTHUS_DIST_CLASSPATH"

  /**
   * Find the user-defined Spark jar if configured, or return the jar containing this
   * class if not.
   *
   * This method first looks in the SparkConf object for the CONF_SPARK_JAR key, and in the
   * user environment if that is not found (for backwards compatibility).
   */
  private def applicationJar(conf: ApplicationConf): String = {
    if (conf.contains(CONF_MAMMUTHUS_YARN_CLIENT__JAR)) {
      conf.get(CONF_MAMMUTHUS_YARN_CLIENT__JAR)
    } else if (System.getenv(ENV_MAMMUTHUS_YARN_CLIENT__JAR) != null) {
      logWarning(
        s"$ENV_MAMMUTHUS_YARN_CLIENT__JAR detected in the system environment. This variable has been deprecated " +
          s"in favor of the $CONF_MAMMUTHUS_YARN_CLIENT__JAR configuration variable.")
      System.getenv(ENV_MAMMUTHUS_YARN_CLIENT__JAR)
    } else {
      ApplicationContext.jarOfClass(this.getClass).head
    }
  }

  /**
   * Return the path to the given application's staging directory.
   */
  private def getAppStagingDir(appId: ApplicationId): String = {
    buildPath(MAMMUTHUS_YARN_CLIENT_STAGING, appId.toString())
  }

  /**
   * Populate the classpath entry in the given environment map with any application
   * classpath specified through the Hadoop and Yarn configurations.
   */
  private def populateHadoopClasspath(conf: Configuration, env: HashMap[String, String])
  : Unit = {
    val classPathElementsToAdd = getYarnAppClasspath(conf) ++ Some(Seq[String]())
    for (c <- classPathElementsToAdd.flatten) {
      YarnHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, c.trim)
    }
  }

  private def getYarnAppClasspath(conf: Configuration): Option[Seq[String]] =
    Option(conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH)) match {
      case Some(s) => Some(s.toSeq)
      case None => getDefaultYarnApplicationClasspath
    }


  private def getDefaultYarnApplicationClasspath: Option[Seq[String]] = {
    val triedDefault = Try[Seq[String]] {
      val field = classOf[YarnConfiguration].getField("DEFAULT_YARN_APPLICATION_CLASSPATH")
      val value = field.get(null).asInstanceOf[Array[String]]
      value.toSeq
    } recoverWith {
      case e: NoSuchFieldException => Success(Seq.empty[String])
    }

    triedDefault match {
      case f: Failure[_] =>
        logError("Unable to obtain the default YARN Application classpath.", f.exception)
      case s: Success[Seq[String]] =>
        logDebug(s"Using the default YARN application classpath: ${s.get.mkString(",")}")
    }

    triedDefault.toOption
  }


  /**
   * Populate the classpath entry in the given environment map.
   *
   * User jars are generally not added to the JVM's system classpath; those are handled by the AM
   * and executor backend. When the deprecated `spark.yarn.user.classpath.first` is used, user jars
   * are included in the system classpath, though. The extra class path and other uploaded files are
   * always made available through the system class path.
   *
   * @param args Client arguments (when starting the AM) or null (when starting executors).
   */
  def populateClasspath(
                         args: ClientArguments,
                         conf: Configuration,
                         applicationConf: ApplicationConf,
                         env: HashMap[String, String],
                         extraClassPath: Option[String] = None): Unit = {
    extraClassPath.foreach(addClasspathEntry(_, env))
    addClasspathEntry(
      YarnHadoopUtil.expandEnvironment(Environment.PWD), env
    )

    addFileToClasspath(new URI(mammuthusJar(applicationConf)), MAMMUTHUS_YARN_CLIENT_JAR, env)
    addFileToClasspath(new URI(APP_JAR), APP_JAR, env)
    populateHadoopClasspath(conf, env)
    sys.env.get(ENV_DIST_CLASSPATH).foreach(addClasspathEntry(_, env))
  }

  /**
   * Returns a list of URIs representing the user classpath.
   *
   * @param conf Spark configuration.
   */
  def getUserClasspath(conf: ApplicationConf): Array[URI] = {
    getUserClasspath(conf.getOption(CONF_MAMMUTHUS_YARN_CLIENT_USER_JAR),
      conf.getOption(CONF_MAMMUTHUS_YARN_CLIENT_YARN_SECONDARY_JARS))
  }

  private def mammuthusJar(conf: ApplicationConf): String = {
    ApplicationContext.jarOfClass(this.getClass).head
  }

  private def getUserClasspath(
                                mainJar: Option[String],
                                secondaryJars: Option[String]): Array[URI] = {
    val mainUri = mainJar.orElse(Some(APP_JAR)).map(new URI(_))
    val secondaryUris = secondaryJars.map(_.split(",")).toSeq.flatten.map(new URI(_))
    (mainUri ++ secondaryUris).toArray
  }

  /**
   * Adds the given path to the classpath, handling "local:" URIs correctly.
   *
   * If an alternate name for the file is given, and it's not a "local:" file, the alternate
   * name will be added to the classpath (relative to the job's work directory).
   *
   * If not a "local:" file and no alternate name, the environment is not modified.
   *
   * @param uri       URI to add to classpath (optional).
   * @param fileName  Alternate name for the file (optional).
   * @param env       Map holding the environment variables.
   */
  private def addFileToClasspath(
                                  uri: URI,
                                  fileName: String,
                                  env: HashMap[String, String]): Unit = {
    if (uri != null && uri.getScheme == LOCAL_SCHEME) {
      addClasspathEntry(uri.getPath, env)
    } else if (fileName != null) {
      addClasspathEntry(buildPath(
        YarnHadoopUtil.expandEnvironment(Environment.PWD), fileName), env)
    }
  }

  /**
   * Add the given path to the classpath entry of the given environment map.
   * If the classpath is already set, this appends the new path to the existing classpath.
   */
  private def addClasspathEntry(path: String, env: HashMap[String, String]): Unit =
    YarnHadoopUtil.addPathToEnvironment(env, Environment.CLASSPATH.name, path)

  /**
   * Get the list of namenodes the user may access.
   */
  private def getNameNodesToAccess(sparkConf: ApplicationConf): Set[Path] = {
    sparkConf.get("mammuthus.yarn.access.namenodes", "")
      .split(",")
      .map(_.trim())
      .filter(!_.isEmpty)
      .map(new Path(_))
      .toSet
  }


  /**
   * Return whether the two file systems are the same.
   */
  private def compareFs(srcFs: FileSystem, destFs: FileSystem): Boolean = {
    val srcUri = srcFs.getUri()
    val dstUri = destFs.getUri()
    if (srcUri.getScheme() == null || srcUri.getScheme() != dstUri.getScheme()) {
      return false
    }

    var srcHost = srcUri.getHost()
    var dstHost = dstUri.getHost()

    // In HA or when using viewfs, the host part of the URI may not actually be a host, but the
    // name of the HDFS namespace. Those names won't resolve, so avoid even trying if they
    // match.
    if (srcHost != null && dstHost != null && srcHost != dstHost) {
      try {
        srcHost = InetAddress.getByName(srcHost).getCanonicalHostName()
        dstHost = InetAddress.getByName(dstHost).getCanonicalHostName()
      } catch {
        case e: UnknownHostException =>
          return false
      }
    }

    Objects.equal(srcHost, dstHost) && srcUri.getPort() == dstUri.getPort()
  }

  /**
   * Given a local URI, resolve it and return a qualified local path that corresponds to the URI.
   * This is used for preparing local resources to be included in the container launch context.
   */
  private def getQualifiedLocalPath(localURI: URI, hadoopConf: Configuration): Path = {
    val qualifiedURI =
      if (localURI.getScheme == null) {
        // If not specified, assume this is in the local filesystem to keep the behavior
        // consistent with that of Hadoop
        new URI(FileSystem.getLocal(hadoopConf).makeQualified(new Path(localURI)).toString)
      } else {
        localURI
      }
    new Path(qualifiedURI)
  }

  /**
   * Whether to consider jars provided by the user to have precedence over the Spark jars when
   * loading user classes.
   */
  def isUserClassPathFirst(conf: ApplicationConf, isDriver: Boolean): Boolean = {
    if (isDriver) {
      conf.getBoolean("mammthus.driver.userClassPathFirst", false)
    } else {
      conf.getBoolean("mammthus.executor.userClassPathFirst", false)
    }
  }

  /**
   * Joins all the path components using Path.SEPARATOR.
   */
  def buildPath(components: String*): String = {
    components.mkString(Path.SEPARATOR)
  }

}
