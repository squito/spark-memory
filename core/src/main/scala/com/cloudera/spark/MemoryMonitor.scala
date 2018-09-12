// (c) Copyright 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.spark

import java.lang.management._
import java.math.{RoundingMode, MathContext}
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean, AtomicReference}
import java.util.concurrent._

import scala.collection.JavaConverters._

import com.quantifind.sumac.FieldArgs

import org.apache.spark.{TaskContext, SparkContext}
import org.apache.spark.ExecutorPlugin
import org.apache.spark.executor.ProcfsBasedMetrics
import org.apache.spark.memory.SparkMemoryManagerHandle

class MemoryMonitor(val args: MemoryMonitorArgs) {
  val nettyMemoryHandle = SparkNettyMemoryHandle.get()
  val sparkMemManagerHandle = SparkMemoryManagerHandle.get()
  val memoryBean = ManagementFactory.getMemoryMXBean
  val poolBeans = ManagementFactory.getMemoryPoolMXBeans.asScala
  val offHeapPoolBeans = poolBeans.filter { pool =>
    // a heuristic which seems OK?
    !pool.isCollectionUsageThresholdSupported && pool.isUsageThresholdSupported
  }
  val memMgrBeans = ManagementFactory.getMemoryManagerMXBeans.asScala
  val bufferPoolsBeans = ManagementFactory.getPlatformMXBeans(classOf[BufferPoolMXBean]).asScala


  // TODO usageThresholds & collection usage thresholds
  // with that setup maybe I'd even just do this for every pool, not just offheap pools

  val getters: Seq[MemoryGetter] =
    Seq(new MemoryMxBeanGetter(memoryBean)) ++
      offHeapPoolBeans.map(new PoolGetter(_)) ++
      bufferPoolsBeans.map(new BufferPoolGetter(_)) ++
      nettyMemoryHandle.toSeq ++
      sparkMemManagerHandle.toSeq ++
      Seq(new ProcfsBasedMetricsGetter)

  val namesAndReporting = getters.flatMap(_.namesAndReporting)
  val names = namesAndReporting.map(_._1)
  val reporting = namesAndReporting.map(_._2).toIndexedSeq
  val nMetrics = namesAndReporting.length
  val getterAndOffset = {
    var offset = 0
    getters.map { g =>
      val thisOffset = offset
      offset += g.namesAndReporting.length
      (g, thisOffset)
    }
  }

  val peakMemoryUsage = new MemoryPeaks(nMetrics)
  val peakUpdates = new PeakUpdate(nMetrics)
  val lastNonShutdownSnapshot = new AtomicReference[MemorySnapshot]()
  val lastThreadDump = new AtomicReference[Array[ThreadInfo]]()
  val inShutdown = new AtomicBoolean(false)

  def showMetricNames: Unit = {
    println(s"${nMetrics} Metrics")
    (0 until nMetrics).foreach { idx => println(names(idx))}
  }

  def collectSnapshot: MemorySnapshot = {
    val now = System.currentTimeMillis()
    val values = new Array[Long](nMetrics)
    getterAndOffset.foreach { case (g, offset) =>
      g.values(values, offset)
    }
    val s = new MemorySnapshot(now, values)
    if (!inShutdown.get()) {
      lastNonShutdownSnapshot.set(s)
      if (args.threadDumpEnabled) {
        lastThreadDump.set(MemoryMonitor.getThreadInfo)
        showLastThreadDump
      }
    }
    s
  }

  def showSnapshot(mem: MemorySnapshot): Unit = {
    println(s"Mem usage at ${MemoryMonitor.dateFormat.format(mem.time)}")
    println("===============")
    // TODO headers for each getter?
    (0 until nMetrics).foreach { idx =>
      val v = mem.values(idx)
      println(names(idx) + "\t:" + MemoryMonitor.bytesToString(v) + "(" + v + ")")
    }
    println()
    println()
  }

  def updateAndMaybeShowPeaks(): Unit = {
    val snapshot = collectSnapshot
    if (peakMemoryUsage.update(snapshot, peakUpdates, reporting)) {
      showUpdates(snapshot.time, peakMemoryUsage, peakUpdates)
    }
  }

  def showUpdates(time: Long, peakMemory: MemoryPeaks, updates: PeakUpdate): Unit = {
    println(s"Peak Memory updates:${MemoryMonitor.dateFormat.format(time)}")
    (0 until updates.nUpdates).foreach { updateIdx =>
      val metricIdx = updates.updateIdx(updateIdx)
      val name = names(metricIdx)
      val currentVal = MemoryMonitor.bytesToString(peakMemoryUsage.values(metricIdx))
      val rawDelta = updates.delta(updateIdx)
      val delta = (if (rawDelta > 0) "+" else "-") + MemoryMonitor.bytesToString(rawDelta)
      println(s"$name\t:\t$currentVal ($delta)")
    }
  }

  def showPeaks(time: Long): Unit = {
    println(s"Peak Memory usage so far ${MemoryMonitor.dateFormat.format(time)}")
    // TODO headers for each getter?
    (0 until nMetrics).foreach { idx =>
      println(names(idx) + "\t:" + MemoryMonitor.bytesToString(peakMemoryUsage.values(idx)) +
        "\t\t\t\t" + MemoryMonitor.dateFormat.format(peakMemoryUsage.peakTimes(idx)))
    }
  }

  def showCurrentMemUsage: Unit = {
    showSnapshot(collectSnapshot)
  }

  def showLastThreadDump: Unit = {
    val threads = lastThreadDump.get()
    if (threads != null) {
      println("last thread dump:")
      MemoryMonitor.showThreadDump(threads)
    }
  }

  def installShutdownHook: Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread(){
      override def run(): Unit = {
        inShutdown.set(true)
        println()
        println("IN SHUTDOWN")
        println("================")
        val snapshot = collectSnapshot
        showSnapshot(snapshot)
        peakMemoryUsage.update(snapshot, peakUpdates, reporting)
        showPeaks(snapshot.time)
        println("Last non-shutdown snapshot:")
        showSnapshot(lastNonShutdownSnapshot.get())

        showLastThreadDump
      }
    })
  }

  def beanInfo(): Unit = {

    memMgrBeans.foreach { mgr =>
      println(mgr.getName + " is managing " + mgr.getMemoryPoolNames.mkString(","))
    }

    poolBeans.foreach { pool =>
      println(pool.getName())
      println("============")
      println(pool.getName() + " is managed by " + pool.getMemoryManagerNames.mkString(","))
      if (pool.isUsageThresholdSupported)
        println("supports usage threshold")
      if (pool.isCollectionUsageThresholdSupported)
        println("supports collection usage threshold")
      pool.getUsage
      println()
      println()
    }

    println("BUFFER POOLS")
    bufferPoolsBeans.foreach { bp =>
      println(s"${bp.getName}: ${bp.getMemoryUsed} / ${bp.getTotalCapacity}")
    }
  }
}

object MemoryMonitor {

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")

  private var monitor: MemoryMonitor = null
  private var shutdownHookInstalled = false
  private var scheduler: ScheduledThreadPoolExecutor = _
  def install(args: MemoryMonitorArgs): MemoryMonitor = synchronized {
    if (monitor == null) {
      monitor = new MemoryMonitor(args)
    }
    monitor
  }

  def installShutdownHook(): Unit = synchronized {
    if (!shutdownHookInstalled) {
      monitor.installShutdownHook
      shutdownHookInstalled = true
    }
  }

  def startPolling(args: MemoryMonitorArgs): Unit = synchronized {
    if (scheduler == null) {
      scheduler = new ScheduledThreadPoolExecutor(1, new ThreadFactory {
        override def newThread(r: Runnable): Thread = {
          val t = new Thread(r, "memory-poll thread")
          t.setDaemon(true)
          t
        }
      })
    }
    scheduler.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        if (args.showEverySnapshot) {
          monitor.showCurrentMemUsage
        } else {
          monitor.updateAndMaybeShowPeaks
        }
      }
    }, 0, args.freq.get, TimeUnit.MILLISECONDS)
  }

  def listAllMBeans: Unit = {
    val server = ManagementFactory.getPlatformMBeanServer
    val allBeans = server.queryNames(null, null)
    println("ALL BEANS")
    println("=============")
    allBeans.asScala.map{_.toString}.toArray.sorted.foreach { ob => println(ob) }
    println()
    println()
  }

  def showLimits: Unit = {
    println("sun.misc.VM.maxDirectMemory(): " + sun.misc.VM.maxDirectMemory())
    println("Runtime.getRuntime.maxMemory(): " + Runtime.getRuntime.maxMemory())
  }

  def installIfSysProps(): Unit = {
    val args = MemoryMonitorArgs.sysPropsArgs
    if (args.enabled) {
      install(args)
      installShutdownHook()
      args.freq.foreach { freq =>
        println(s"POLLING memory monitor every $freq millis")
        monitor.showCurrentMemUsage
        println("done with initial show")
        startPolling(args)
      }
    }
  }

  def installOnExecIfStaticAllocation(sc: SparkContext): Unit = {
    if (!SparkMemoryManagerHandle.isDynamicAllocation(sc)) {
      installOnExecutors(sc)
    } else {
      println ("********* WARNING ***** not installing on executors because of DA")
    }
  }

  def installOnExecutors(sc: SparkContext, numTasks: Int = -1, sleep: Long = 1): Unit = {
    assert(!SparkMemoryManagerHandle.isDynamicAllocation(sc))
    val t = if (numTasks == -1) {
      sc.getExecutorMemoryStatus.size * 2
    } else {
      numTasks
    }
    println(s"Running $t tasks to install memory monitor on executors")
    sc.parallelize(1 to t, t).foreach { _ =>
      Thread.sleep(sleep)
      installIfSysProps()
    }
  }

  /**
    * Convert a quantity in bytes to a human-readable string such as "4.0 MB".
    */
  def bytesToString(size: Long): String = bytesToString(BigInt(size))

  def bytesToString(size: BigInt): String = {
    val EB = 1L << 60
    val PB = 1L << 50
    val TB = 1L << 40
    val GB = 1L << 30
    val MB = 1L << 20
    val KB = 1L << 10

    if (size >= BigInt(1L << 11) * EB) {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    } else {
      val (value, unit) = {
        if (size >= 2 * EB) {
          (BigDecimal(size) / EB, "EB")
        } else if (size >= 2 * PB) {
          (BigDecimal(size) / PB, "PB")
        } else if (size >= 2 * TB) {
          (BigDecimal(size) / TB, "TB")
        } else if (size >= 2 * GB) {
          (BigDecimal(size) / GB, "GB")
        } else if (size >= 2 * MB) {
          (BigDecimal(size) / MB, "MB")
        } else if (size >= 2 * KB) {
          (BigDecimal(size) / KB, "KB")
        } else {
          (BigDecimal(size), "B")
        }
      }
      "%.1f %s".formatLocal(Locale.US, value, unit)
    }
  }

  def getThreadInfo: Array[ThreadInfo] = {
    // I'm avoiding getting locks for the moment in a random hope that it might be faster,
    // and because I don't really care right now
    ManagementFactory.getThreadMXBean.dumpAllThreads(false, false)
  }

  def showThreadDump(threads: Array[ThreadInfo]): Unit = {
    threads.foreach { t =>
      if (t == null) {
        println("<null thread>")
      } else {
        println(t.getThreadId + " " + t.getThreadName + " " + t.getThreadState)
        t.getStackTrace.foreach { elem => println("\t" + elem) }
        println()
      }
    }
  }
}

class MemoryMonitorExecutorExtension extends ExecutorPlugin {
  // Each Spark executor will create an instance of this plugin. When this class
  // is instantiated, this static method is called, which is good enough for us.
  MemoryMonitor.installIfSysProps()
  /*
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
  */
}

class MemoryMonitorArgs extends FieldArgs {
  var enabled = false
  // java.lang.Long because scalac makes Option[Long] look like Option[Any] to java reflection
  var freq: Option[java.lang.Long] = None
  var showEverySnapshot = false

  var stagesToPoll: Array[Int] = _

  var threadDumpFreqMillis: Int = 1000
  var threadDumpEnabled = false

  var verbose = false
}

object MemoryMonitorArgs {
  val prefix = "memory.monitor."
  val prefixLen = prefix.length

  lazy val sysPropsArgs = {
    val args = new MemoryMonitorArgs
    args.parse(sys.props.collect { case (k,v) if k.startsWith(prefix) =>
        k.substring(prefixLen) -> v
    })
    if (args.stagesToPoll != null && args.stagesToPoll.nonEmpty) {
      System.out.println(s"will poll thread dumps for stages ${args.stagesToPoll.mkString(",")}")
    } else {
      args.stagesToPoll = Array()
    }
    args
  }
}
