// (c) Copyright 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.spark

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

object MemExample {

  def main(args: Array[String]): Unit = {
    println(ManagementFactory.getRuntimeMXBean.getSpecVersion)
    println(ManagementFactory.getRuntimeMXBean.getVmVersion)
    MemoryMonitor.listAllMBeans
    MemoryMonitor.showLimits
    val monitor = new MemoryMonitor()
    monitor.beanInfo()
    monitor.showMetricNames
    monitor.showCurrentMemUsage

    val buffers = ArrayBuffer[ByteBuffer]()

    while (true) {
      buffers += ByteBuffer.allocateDirect(1e8.toInt)
      monitor.updateAndMaybeShowPeaks()
    }
  }
}
