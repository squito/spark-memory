// (c) Copyright 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.spark

import java.lang.management.{BufferPoolMXBean, MemoryMXBean, MemoryPoolMXBean}

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
