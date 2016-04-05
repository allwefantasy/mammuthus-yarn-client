package mammuthus.yarn.exception

/**
 * 8/20/15 WilliamZhu(allwefantasy@gmail.com)
 */
class ApplicationException(message: String, cause: Throwable)
  extends Exception(message, cause) {

  def this(message: String) = this(message, null)
}
