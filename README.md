# mammuthus-yarn-client

mammuthus-yarn-client 后续为了方便，我们就缩写为MYC。 

## 概述

MYC 主要源码来自于 Spark的yarn模块。通过对其进行改造，使得其后续可以作为一个通用的Yarn项目开发框架。
目前MYC还比较简单，但不影响你基于它非常快的构建出一个基于Yarn的分布式应用。


## 基于MYC 开发基本原理

基于Yarn开发的应用大体是一个典型Master-Slaves结构。其中Master 在Yarn中称为 ApplicationMaster. ApplicationMaster代表应用和
ResourceManager进行交互。在MYC中提供了一个公用的ApplicationMaster实现，用户的Master则在在AM中的一个单独线程中被启动，启动完成后
AM会获取到Master的地址和端口，然后将这些信息传递给应用对象的Slave，Slave从而根据这些信息和Master获取联系。

通常Slave 和 Master的通讯方式有两种：

1. 通过一个中间组件进行通讯，比如通过zookeeper来完成。
2. Slave 主动向Master发送心跳，Master维持关系


前面我们提到，Master被AM启动后，AM会获取到Master的地址和端口，这就需要一种途径传递这些信息。MYC定义了一些接口，只要Master/Slave分别实现这些接口，就能完成这些基础功能。

### 实现Master

为了能够和你的Master进行交互，需要保证你的Master启动类实现

```java
mammuthus.yarn.MammuthusApplication
```
接口规范。该类具体待实现方法如下：

```java
trait MammuthusApplication {

  def masterPort: Int //应用开启的端口

  def uiAddress: String // Master的管理界面地址

  /*
    args 来自于--args 参数（启动命令中附件的参数）。MYC 会通过args传递给你。 
    maps 参数主要为了后续进一步和MYC进行交互，譬如你可以将一些参数通过maps传递给MYC
  */
  def run(args: Array[String], maps: java.util.Map[String, String]):Unit

}
```

1. 提供你的端口号
2. 管理页面地址
3. 在run方法里初始化你的应用

实例代码如下来自[DynamicDeployMaster.scala](https://github.com/allwefantasy/mammuthus-dynamic-deploy/blob/master/src/main/java/mammuthus/deploy/dynamic/DynamicDeployMaster.scala):

```
//启动ServiceFramework框架
object DynamicDeployMaster {
  var  mammuthusMasterClient:TaskService = _
  def main(args: Array[String]): Unit = {
    ServiceFramwork.scanService.setLoader(classOf[DynamicDeployMaster])
    ServiceFramwork.applicaionYamlName("application.master.yml")
    ServiceFramwork.disableDubbo()
    ServiceFramwork.enableNoThreadJoin()
    Application.main(args)
  }

}

//实现MammuthusApplication 接口，保证AM能够拿到端口，UI地址等
class DynamicDeployMaster extends MammuthusApplication {

  var httpPort: Int = 0
  var _uiAddress = ""
  var mammuthusMasterAddress:String = _


  override def masterPort: Int = httpPort

  override def uiAddress: String = _uiAddress

  override def run(args: Array[String], maps: java.util.Map[String, String]): Unit = {
    DynamicDeployMaster.main(args)
    
    val hp = ServiceFramwork.injector.getInstance(classOf[HttpServer])
    httpPort = hp.getHttpPort
    val containerService = ServiceFramwork.injector.getInstance(classOf[DynamicMasterInfoService])
    containerService.imageUrl = args(0)
    containerService.location = args(1)
    containerService.startCommand = args(2)
    mammuthusMasterAddress = args(3)
    
    maps.put("httpPort", masterPort + "")
    maps.put("uiAddress", s"http://${InetAddress.getLocalHost.getHostName}:${masterPort}")
    
    _uiAddress = maps.get("uiAddress")
    
    Thread.currentThread().join()
  }



  private def driverHostAndPort = {
    val uri = URI.create(mammuthusMasterAddress)
    uri.getHost + ":" + uri.getPort
  }
}
```

通过该类完成一个常驻程序。

### 实现你的Slave

同样的，为了能够让MYC能够启动你的Slave程序，你需要遵循一些接口规范。你的启动类需要实现如下接口：

```java
mammuthus.yarn.ExecutorBackend
```

你的Slave启动类会得到一个如下的对象：

```scala
case class ExecutorBackendParam(driverUrl: String,
                                slaveClass: String,
                                executorId: String,
                                hostname: String,
                                cores: Int,
                                executeMemory: String,
                                appId: String,
                                userClassPath: mutable.ListBuffer[URL]
                                 )
```


具体例子如下[DynamicDeploySlave.scala](https://github.com/allwefantasy/mammuthus-dynamic-deploy/blob/master/src/main/java/mammuthus/deploy/dynamic/DynamicDeploySlave.scala):

```scala
//启动ServiceFramework框架
object DynamicDeploySlave extends ExecutorBackend {
  var applicationMasterArguments: ExecutorBackendParam = null

  def main(args: Array[String]) = {
    applicationMasterArguments = parse(args)
    ServiceFramwork.applicaionYamlName("application.slave.yml")
    ServiceFramwork.scanService.setLoader(classOf[DynamicDeploySlave])
    ServiceFramwork.disableDubbo()
    ServiceFramwork.registerStartWithSystemServices(classOf[HeartBeatService])
    Application.main(args)
  }
}

class DynamicDeploySlave

```


## 运行方式

完成你的应用开发后，package成一个jar包，然后就可以通过一个java命令完成向Yarn集群提交任务的步骤了。

参考 [mammuthus-yarn-docker-scheduler](https://github.com/allwefantasy/mammuthus-yarn-docker-scheduler)项目的说明，大体来说如下：

```
java -cp /Users/allwefantasy/CSDNWorkSpace/mammuthus-yarn-client/target/mammuthus-yarn-client-1.0-SNAPSHOT-SHADED.jar mammuthus.yarn.Client \
--jar /Users/allwefantasy/CSDNWorkSpace/mammuthus-dynamic-deploy/target/mammuthus-dynamic-deploy-1.0-SNAPSHOT-jar-with-dependencies.jar \ 
--driver-memory 256m \
--num-executors 2 \
--executor-memory 512m  \
--class mammuthus.deploy.dynamic.DynamicDeployMaster \
--slave-class mammuthus.deploy.dynamic.DynamicDeploySlave \
--arg "http://appstore/DCS@1.0.tar.gz" \
--arg "/tmp/DCS-dev@1.0.tar.gz" \
--arg "./app.sh launch_dcs.sh start" \
--arg "http://appstore" 
```

其中 
--jar，应用程序jar包地址
--dirver-memory, master 数量
--num-executors，Slave数量
--executor-memory， Slave 内存大小
--class, Master 启动类
--slave-class ， Slave 启动类 

--args 则是你应用需要接受的参数。




