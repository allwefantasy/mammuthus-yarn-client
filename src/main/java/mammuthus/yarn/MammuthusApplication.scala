package mammuthus.yarn

/**
 * 4/2/16 WilliamZhu(allwefantasy@gmail.com)
 */
trait MammuthusApplication {

  def masterPort: Int

  def uiAddress: String

  def run(args: Array[String], maps: java.util.Map[String, String]):Unit

}
