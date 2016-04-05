package mammuthus.yarn.util

/**
 * 8/31/15 WilliamZhu(allwefantasy@gmail.com)
 */
case class ThreadStackTrace(
                             threadId: Long,
                             threadName: String,
                             threadState: Thread.State,
                             stackTrace: String)
