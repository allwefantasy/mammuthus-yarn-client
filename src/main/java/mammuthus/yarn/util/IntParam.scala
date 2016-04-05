package mammuthus.yarn.util

/**
 * 8/20/15 WilliamZhu(allwefantasy@gmail.com)
 */
object IntParam {
  def unapply(str: String): Option[Int] = {
    try {
      Some(str.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }
}
