package mammuthus.yarn.exception

import mammuthus.yarn.util.ApplicationExitCode
import mammuthus.yarn.log.Logging
import mammuthus.yarn.util.Utils


/**
 * 8/20/15 WilliamZhu(allwefantasy@gmail.com)
 */
object ApplicationUncaughtExceptionHandler
  extends Thread.UncaughtExceptionHandler with Logging {

  override def uncaughtException(thread: Thread, exception: Throwable) {
    try {
      logError("Uncaught exception in thread " + thread, exception)

      // We may have been called from a shutdown hook. If so, we must not call System.exit().
      // (If we do, we will deadlock.)
      if (!Utils.inShutdown()) {
        if (exception.isInstanceOf[OutOfMemoryError]) {
          System.exit(ApplicationExitCode.OOM)
        } else {
          System.exit(ApplicationExitCode.UNCAUGHT_EXCEPTION)
        }
      }
    } catch {
      case oom: OutOfMemoryError => Runtime.getRuntime.halt(ApplicationExitCode.OOM)
      case t: Throwable => Runtime.getRuntime.halt(ApplicationExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }

  def uncaughtException(exception: Throwable) {
    uncaughtException(Thread.currentThread(), exception)
  }
}
