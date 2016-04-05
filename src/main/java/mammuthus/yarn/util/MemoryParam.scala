package mammuthus.yarn.util


/**
 * 8/20/15 WilliamZhu(allwefantasy@gmail.com)
 */
object MemoryParam {
  def unapply(str: String): Option[Int] = {
    try {
      Some(Utils.memoryStringToMb(str))
    } catch {
      case e: NumberFormatException => None
    }
  }
}
