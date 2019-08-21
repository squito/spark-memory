package org.apache.spark.memory

import com.cloudera.spark.{Reflector, IncrementBytes, MemoryGetter}
import org.apache.spark.{SparkContext, SparkEnv}

class SparkMemoryManagerHandle(
    val onHeapStorage: StorageMemoryPool,
    val offHeapStorage: StorageMemoryPool,
    val onHeapExecution: ExecutionMemoryPool,
    val offHeapExecution: ExecutionMemoryPool,
    val lock: Object) extends MemoryGetter {

  val namesAndReporting = for {
    t <- Seq("onHeap", "offHeap")
    s <- Seq("Storage", "Execution", "MaxTaskExecution")
  } yield {
    (s"$t$s", IncrementBytes)
  }

  override def values(dest: Array[Long], offset: Int): Unit = {
    dest(offset) = onHeapStorage.memoryUsed
    dest(offset + 1) = onHeapExecution.memoryUsed
    dest(offset + 2) = maxTaskExecution(onHeapExecution)
    dest(offset + 3) = offHeapStorage.memoryUsed
    dest(offset + 4) = offHeapExecution.memoryUsed
    dest(offset + 5) = maxTaskExecution(offHeapExecution)
  }

  def maxTaskExecution(executionMemoryPool: ExecutionMemoryPool): Long = {
    // This is totally hacking into internals, even locks ...
    import Reflector._
    lock.synchronized {
      val taskMem = executionMemoryPool
        .reflectField("memoryForTask").asInstanceOf[scala.collection.Map[Long, Long]]
      if (taskMem.nonEmpty) {
        taskMem.values.max
      } else {
        0L
      }
    }
  }
}

object SparkMemoryManagerHandle {
  def get(displayError: Boolean = false): Option[SparkMemoryManagerHandle] = try {
    val env = SparkEnv.get
    val memManager = env.memoryManager
    import Reflector._
    Some(new SparkMemoryManagerHandle(
      memManager.reflectField("onHeapStorageMemoryPool").asInstanceOf[StorageMemoryPool],
      memManager.reflectField("offHeapStorageMemoryPool").asInstanceOf[StorageMemoryPool],
      memManager.reflectField("onHeapExecutionMemoryPool").asInstanceOf[ExecutionMemoryPool],
      memManager.reflectField("offHeapExecutionMemoryPool").asInstanceOf[ExecutionMemoryPool],
      memManager
    ))
  } catch {
    case ex: Exception =>
      if (displayError) {
        ex.printStackTrace()
      }
      None
  }

  def isDynamicAllocation(sc: SparkContext): Boolean = {
    org.apache.spark.util.Utils.isDynamicAllocationEnabled(sc.getConf)
  }
}
