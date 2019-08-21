package com.cloudera.spark

import org.scalatest.FunSuite

class MemoryMonitorExecutorExtensionSuite  extends FunSuite {
  test("MemoryMonitorExecutorExtension should extend the correct class of ExecutorPlugin") {
    assert(classOf[MemoryMonitorExecutorExtension].getInterfaces.contains(classOf[org.apache.spark.executor.ExecutorPlugin]))
    assert(classOf[MemoryMonitorExecutorExtension].getInterfaces.contains(classOf[org.apache.spark.ExecutorPlugin]))
  }
}
