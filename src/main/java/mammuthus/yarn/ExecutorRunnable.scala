package mammuthus.yarn

import java.io.File
import java.net.URI
import java.nio.ByteBuffer

import mammuthus.yarn.exception.ApplicationException
import mammuthus.yarn.log.Logging
import mammuthus.yarn.util.{Utils, YarnHadoopUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.NMClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{ConverterUtils, Records}

import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap, ListBuffer}

class ExecutorRunnable(
                        container: Container,
                        conf: Configuration,
                        appConf: ApplicationConf,
                        masterAddress: String,
                        slaveClass: String,
                        slaveId: String,
                        hostname: String,
                        executorMemory: Int,
                        executorCores: Int,
                        appId: String)
  extends Runnable with Logging {

  var rpc: YarnRPC = YarnRPC.create(conf)
  var nmClient: NMClient = _
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  lazy val env = prepareEnvironment(container)

  def run = {
    logInfo("Starting Executor Container")
    nmClient = NMClient.createNMClient()
    nmClient.init(yarnConf)
    nmClient.start()
    startContainer
  }

  def startContainer = {
    logInfo("Setting up ContainerLaunchContext")

    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
      .asInstanceOf[ContainerLaunchContext]

    val localResources = prepareLocalResources
    ctx.setLocalResources(localResources)

    ctx.setEnvironment(env)

    val credentials = UserGroupInformation.getCurrentUser().getCredentials()
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    ctx.setTokens(ByteBuffer.wrap(dob.getData()))

    val commands = prepareCommand(masterAddress, slaveId, slaveClass, hostname, executorMemory, executorCores,
      appId, localResources)

    logInfo(s"Setting up executor with environment: $env")
    logInfo("Setting up executor with commands: " + commands)
    ctx.setCommands(commands)

    // Send the start request to the ContainerManager
    try {
      nmClient.startContainer(container, ctx)
    } catch {
      case ex: Exception =>
        throw new ApplicationException(s"Exception while starting container ${container.getId}" +
          s" on host $hostname", ex)
    }
  }

  private def prepareCommand(
                              masterAddress: String,
                              slaveId: String,
                              slaveClass: String,
                              hostname: String,
                              executorMemory: Int,
                              executorCores: Int,
                              appId: String,
                              localResources: HashMap[String, LocalResource]): List[String] = {
    // Extra options for the JVM
    val javaOpts = ListBuffer[String]()

    // Set the environment variable through a command prefix
    // to append to the existing value of the variable
    var prefixEnv: Option[String] = None

    // Set the JVM memory
    //val executorMemoryString = executorMemory + "m"
    javaOpts += "-Xms128m"
    javaOpts += "-Xmx256m"

    // Set extra Java options for the executor, if defined
    sys.props.get("mammuthus.executor.extraJavaOptions").foreach {
      opts =>
        javaOpts ++= Utils.splitCommandString(opts).map(YarnHadoopUtil.escapeForShell _)
    }
    sys.env.get("MAMMUTHUS_JAVA_OPTS").foreach {
      opts =>
        javaOpts ++= Utils.splitCommandString(opts).map(YarnHadoopUtil.escapeForShell _)
    }
    sys.props.get("mammuthus.executor.extraLibraryPath").foreach {
      p =>
        prefixEnv = Some(Utils.libraryPathEnvPrefix(Seq(p)))
    }

    javaOpts += "-Djava.io.tmpdir=" +
      new Path(
        YarnHadoopUtil.expandEnvironment(Environment.PWD),
        YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR
      )

    // For log4j configuration to reference
    javaOpts += ("-Dmammuthus.yarn.app.container.log.dir=" + ApplicationConstants.LOG_DIR_EXPANSION_VAR)

    val userClassPath = Client.getUserClasspath(appConf).flatMap {
      uri =>
        val absPath =
          if (new File(uri.getPath()).isAbsolute()) {
            uri.getPath()
          } else {
            Client.buildPath(Environment.PWD.$(), uri.getPath())
          }
        Seq("--user-class-path", "file:" + absPath)
    }.toSeq

    val commands = prefixEnv ++ Seq(
      YarnHadoopUtil.expandEnvironment(Environment.JAVA_HOME) + "/bin/java",
      "-server",
      // Kill if OOM is raised - leverage yarn's failure handling to cause rescheduling.
      // Not killing the task leaves various aspects of the executor and (to some extent) the jvm in
      // an inconsistent state.
      // TODO: If the OOM is not recoverable by rescheduling it on different node, then do
      // 'something' to fail job ... akin to blacklisting trackers in mapred ?
      "-XX:OnOutOfMemoryError='kill %p'") ++
      javaOpts ++
      Seq(slaveClass,
        "--driver-url", masterAddress,
        "--executor-id", slaveId.toString,
        "--hostname", hostname.toString,
        "--execute-memory", executorMemory.toString,
        "--cores", executorCores.toString,
        "--app-id", appId) ++
      userClassPath ++
      Seq(
        "1>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout",
        "2>", ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")

    // TODO: it would be nicer to just make sure there are no null commands here
    commands.map(s => if (s == null) "null" else s).toList
  }

  private def setupDistributedCache(
                                     file: String,
                                     rtype: LocalResourceType,
                                     localResources: HashMap[String, LocalResource],
                                     timestamp: String,
                                     size: String,
                                     vis: String): Unit = {
    val uri = new URI(file)
    val amJarRsrc = Records.newRecord(classOf[LocalResource])
    amJarRsrc.setType(rtype)
    amJarRsrc.setVisibility(LocalResourceVisibility.valueOf(vis))
    amJarRsrc.setResource(ConverterUtils.getYarnUrlFromURI(uri))
    amJarRsrc.setTimestamp(timestamp.toLong)
    amJarRsrc.setSize(size.toLong)
    localResources(uri.getFragment()) = amJarRsrc
  }

  private def prepareLocalResources: HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val localResources = HashMap[String, LocalResource]()

    if (System.getenv("MAMMUTHUS_YARN_CACHE_FILES") != null) {
      val timeStamps = System.getenv("MAMMUTHUS_YARN_CACHE_FILES_TIME_STAMPS").split(',')
      val fileSizes = System.getenv("MAMMUTHUS_YARN_CACHE_FILES_FILE_SIZES").split(',')
      val distFiles = System.getenv("MAMMUTHUS_YARN_CACHE_FILES").split(',')
      val visibilities = System.getenv("MAMMUTHUS_YARN_CACHE_FILES_VISIBILITIES").split(',')
      for (i <- 0 to distFiles.length - 1) {
        setupDistributedCache(distFiles(i), LocalResourceType.FILE, localResources, timeStamps(i),
          fileSizes(i), visibilities(i))
      }
    }

    if (System.getenv("MAMMUTHUS_YARN_CACHE_ARCHIVES") != null) {
      val timeStamps = System.getenv("MAMMUTHUS_YARN_CACHE_ARCHIVES_TIME_STAMPS").split(',')
      val fileSizes = System.getenv("MAMMUTHUS_YARN_CACHE_ARCHIVES_FILE_SIZES").split(',')
      val distArchives = System.getenv("MAMMUTHUS_YARN_CACHE_ARCHIVES").split(',')
      val visibilities = System.getenv("MAMMUTHUS_YARN_CACHE_ARCHIVES_VISIBILITIES").split(',')
      for (i <- 0 to distArchives.length - 1) {
        setupDistributedCache(distArchives(i), LocalResourceType.ARCHIVE, localResources,
          timeStamps(i), fileSizes(i), visibilities(i))
      }
    }

    logInfo("Prepared Local resources " + localResources)
    localResources
  }

  private def prepareEnvironment(container: Container): HashMap[String, String] = {
    val env = new HashMap[String, String]()
    val extraCp = appConf.getOption("mammuthus.executor.extraClassPath")
    Client.populateClasspath(null, yarnConf, appConf, env, extraCp)

    appConf.getExecutorEnv.foreach {
      case (key, value) =>
        // This assumes each executor environment variable set here is a path
        // This is kept for backward compatibility and consistency with hadoop
        YarnHadoopUtil.addPathToEnvironment(env, key, value)
    }

    // Keep this for backwards compatibility but users should move to the config
    sys.env.get("MAMMUTHUS_YARN_USER_ENV").foreach {
      userEnvs =>
        YarnHadoopUtil.setEnvFromInputString(env, userEnvs)
    }

    // Add log urls
    sys.env.get("MAMMUTHUS_USER").foreach {
      user =>
        val baseUrl = "http://%s/node/containerlogs/%s/%s"
          .format(container.getNodeHttpAddress, ConverterUtils.toString(container.getId), user)
        env("MAMMUTHUS_LOG_URL_STDERR") = s"$baseUrl/stderr?start=0"
        env("MAMMUTHUS_LOG_URL_STDOUT") = s"$baseUrl/stdout?start=0"
    }

    System.getenv().filterKeys(_.startsWith("MAMMUTHUS")).foreach {
      case (k, v) => env(k) = v
    }
    env
  }
}