// (c) Copyright 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.spark

import java.lang.management.{MemoryPoolMXBean, MemoryMXBean, BufferPoolMXBean, ManagementFactory}
import java.math.{RoundingMode, MathContext}
import java.util.Locale
import java.util.concurrent.{ThreadFactory, TimeUnit, ScheduledThreadPoolExecutor}

import org.apache.spark.memory.SparkMemoryManagerHandle

import scala.collection.JavaConverters._

import org.apache.spark.SparkContext

class MemoryMonitor {
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
      sparkMemManagerHandle.toSeq

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
    new MemorySnapshot(now, values)
  }

  def showSnapshot(mem: MemorySnapshot): Unit = {
    println(s"Mem usage at ${new java.util.Date(mem.time)}")
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
      showUpdates(peakMemoryUsage, peakUpdates)
    }
  }

  def showUpdates(peakMemory: MemoryPeaks, updates: PeakUpdate): Unit = {
    println(s"Peak Memory updates:${new java.util.Date(System.currentTimeMillis())}")
    (0 until updates.nUpdates).foreach { updateIdx =>
      val metricIdx = updates.updateIdx(updateIdx)
      val name = names(metricIdx)
      val currentVal = MemoryMonitor.bytesToString(peakMemoryUsage.values(metricIdx))
      val rawDelta = updates.delta(updateIdx)
      val delta = (if (rawDelta > 0) "+" else "-") + MemoryMonitor.bytesToString(rawDelta)
      println(s"$name\t:\t$currentVal ($delta)")
    }
  }

  def showPeaks(): Unit = {
    println(s"Peak Memory usage so far ${new java.util.Date(System.currentTimeMillis())}")
    // TODO headers for each getter?
    (0 until nMetrics).foreach { idx =>
      println(names(idx) + "\t:" + MemoryMonitor.bytesToString(peakMemoryUsage.values(idx)) +
        "\t\t\t\t" + new java.util.Date(peakMemoryUsage.peakTimes(idx)))
    }
  }

  def showCurrentMemUsage: Unit = {
    showSnapshot(collectSnapshot)
  }

  def installShutdownHook: Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread(){
      override def run(): Unit = {
        println("IN SHUTDOWN")
        val snapshot = collectSnapshot
        showSnapshot(snapshot)
        peakMemoryUsage.update(snapshot, peakUpdates, reporting)
        showPeaks()
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

  private var monitor: MemoryMonitor = null
  private var shutdownHookInstalled = false
  private var scheduler: ScheduledThreadPoolExecutor = _
  def install(): MemoryMonitor = synchronized {
    if (monitor == null) {
      monitor = new MemoryMonitor()
    }
    monitor
  }

  def installShutdownHook(): Unit = synchronized {
    if (!shutdownHookInstalled) {
      monitor.installShutdownHook
      shutdownHookInstalled = true
    }
  }

  def startPolling(millis: Long): Unit = synchronized {
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
        if (sys.props.get("memory.monitor.all").isDefined) {
          monitor.showCurrentMemUsage
        } else {
          monitor.updateAndMaybeShowPeaks
        }
      }
    }, 0, millis, TimeUnit.MILLISECONDS)
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

  def installIfSysProps(): Option[Any] = {
    sys.props.get("memory.monitor.enabled").flatMap { _ =>
      install()
      installShutdownHook()
      sys.props.get("memory.monitor.freq").map { freq =>
        println(s"POLLING memory monitor every $freq millis")
        monitor.showCurrentMemUsage
        println("done with initial show")
        startPolling(freq.toLong)
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

}


sealed trait PeakReporting {
  def report(orig: Long, update: Long): Boolean
}
case object Never extends PeakReporting {
  override def report(orig: Long, update: Long): Boolean = false
}
case object Always extends PeakReporting {
  override def report(orig: Long, update: Long): Boolean = update > orig
}
case object IncrementBytes extends PeakReporting {
  override def report(orig: Long, update: Long): Boolean = {
    val delta = update - orig
    delta > 1e7.toInt && (delta.toDouble / orig) > 1.05
  }
}

case object IncrementCounts extends PeakReporting {
  override def report(orig: Long, update: Long): Boolean = {
    val delta = update - orig
    delta > 100.toInt && (delta.toDouble / orig) > 1.05
  }
}

trait MemoryGetter {
  def namesAndReporting: Seq[(String, PeakReporting)]
  def values(dest: Array[Long], offset: Int): Unit
}

class MemoryMxBeanGetter(bean: MemoryMXBean) extends MemoryGetter {
  val namesAndReporting: Seq[(String, PeakReporting)] = for {
    source <- Seq("heap", "offheap")
    usage <- Seq(("used", IncrementBytes), ("committed", Always))
  } yield {
    (source + ":" + usage._1, usage._2)
  }
  def values(dest: Array[Long], offset:Int): Unit = {
    val heap = bean.getHeapMemoryUsage()
    dest(offset) = heap.getUsed()
    dest(offset + 1) = heap.getCommitted()
    val offheap = bean.getNonHeapMemoryUsage()
    dest(offset + 2) = offheap.getUsed()
    dest(offset + 3) = offheap.getCommitted()
  }
}

class PoolGetter(bean: MemoryPoolMXBean) extends MemoryGetter {
  val namesAndReporting: Seq[(String, PeakReporting)] =
    Seq(("used", IncrementBytes), ("committed", Always)).map { case (n, r) =>
      (bean.getName() + n, r)
    }
  def values(dest: Array[Long], offset: Int): Unit = {
    // there are actually a bunch more things here I *could* get ...
    val usage = bean.getUsage()
    dest(offset) = usage.getUsed()
    dest(offset + 1) = usage.getCommitted()
  }
}

class BufferPoolGetter(bean: BufferPoolMXBean) extends MemoryGetter {
  val namesAndReporting = Seq(("capacity", IncrementBytes), ("used", IncrementBytes)).map{ case (n, r) =>
    (bean.getName() + ":" + n, r)
  }
  def values(dest: Array[Long], offset: Int): Unit = {
    dest(offset) = bean.getTotalCapacity()
    dest(offset + 1) = bean.getMemoryUsed()
  }
}

class MemoryMonitorExecutorExtension {
  // the "extension class" api just lets you invoke a constructor.  We really just want to
  // call this static method, so that's good enough.
  MemoryMonitor.installIfSysProps()
}
