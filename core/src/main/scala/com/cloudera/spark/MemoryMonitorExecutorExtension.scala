package com.cloudera.spark

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}

import org.apache.spark.TaskContext
import org.apache.spark.executor.ExecutorPlugin

class MemoryMonitorExecutorExtension extends ExecutorPlugin with org.apache.spark.ExecutorPlugin {
  // the "extension class" api just lets you invoke a constructor.  We really just want to
  // call this static method, so that's good enough.
  MemoryMonitor.installIfSysProps()
  val args = MemoryMonitorArgs.sysPropsArgs

  val monitoredTaskCount = new AtomicInteger(0)

  val scheduler = if (args.stagesToPoll != null && args.stagesToPoll.nonEmpty) {
    // TODO share polling executors?
    new ScheduledThreadPoolExecutor(1, new ThreadFactory {
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, "thread-dump poll thread")
        t.setDaemon(true)
        t
      }
    })
  } else {
    null
  }
  val pollingTask = new AtomicReference[ScheduledFuture[_]]()

  override def taskStart(taskContext: TaskContext): Unit = {
    if (args.stagesToPoll.contains(taskContext.stageId())) {
      if (monitoredTaskCount.getAndIncrement() == 0) {
        // TODO schedule thread polling
        val task = scheduler.scheduleWithFixedDelay(new Runnable {
          override def run(): Unit = {
            val d = MemoryMonitor.dateFormat.format(System.currentTimeMillis())
            println(s"Polled thread dump @ $d")
            MemoryMonitor.showThreadDump(MemoryMonitor.getThreadInfo)
          }
        }, 0, args.threadDumpFreqMillis, TimeUnit.MILLISECONDS)
        pollingTask.set(task)
      }
    }
  }

  override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
    removeActiveTask(context)
  }

  override def onTaskCompletion(context: TaskContext): Unit = {
    removeActiveTask(context)
  }

  private def removeActiveTask(context: TaskContext): Unit = {
    if (args.stagesToPoll.contains(context.stageId())) {
      if (monitoredTaskCount.decrementAndGet() == 0) {
        pollingTask.get().cancel(false)
      }
    }
  }
}
