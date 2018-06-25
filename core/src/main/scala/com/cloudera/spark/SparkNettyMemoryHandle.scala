// (c) Copyright 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.spark

import java.lang.reflect.Modifier

import scala.collection.JavaConverters._

import io.netty.buffer._
import io.netty.channel.ChannelOption

import org.apache.spark.SparkEnv

class SparkNettyMemoryHandle(
  val rpcClientPool: PooledByteBufAllocatorMetric,
  val rpcServerPool: PooledByteBufAllocatorMetric,
  val blockClientPool: PooledByteBufAllocatorMetric,
  val blockServerPool: PooledByteBufAllocatorMetric
) extends MemoryGetter {
  override def toString(): String = {
    "RPC Client pool:" + rpcClientPool + "\n" +
      "RPC Server pool:" + rpcServerPool + "\n" +
      "Block Transfer client pool:" + blockClientPool + "\n" +
      "Block Transfer server pool:" + blockServerPool
  }

  val poolMetrics = Seq(
    ("usedHeapMem", IncrementBytes),
    ("usedDirectMem", IncrementBytes),
    ("numHeapArenas", Always),
    ("numDirectArenas", Always),
    ("numThreadLocalCaches", Always))

  val allPooledMetrics = poolMetrics ++ SparkNettyMemoryHandle.VERBOSE_METRICS ++
    Seq(("directAllocatedUnused", IncrementBytes), ("heapAllocationUnused", IncrementBytes))

  val namesAndReporting: Seq[(String, PeakReporting)] = (for {
    pool <- Seq("rpc", "blockTransfer")
    client <- Seq("client", "server")
    (metric, reporting) <- allPooledMetrics
  } yield {
    ("netty-" + pool + "-" + client + "-" + metric, reporting)
  }) ++ Seq(("netty-Unpooled-heapUsed", IncrementBytes), ("netty-Unpooled-directUsed", IncrementBytes))

  def values(dest: Array[Long], initOffset: Int): Unit = {
    var offset = initOffset
    Seq(rpcClientPool, rpcServerPool, blockClientPool, blockServerPool).foreach { pool =>
      dest(offset) = pool.usedHeapMemory()
      dest(offset + 1) = pool.usedDirectMemory()
      dest(offset + 2) = pool.numHeapArenas()
      dest(offset + 3) = pool.numDirectArenas()
      dest(offset + 4) = pool.numThreadLocalCaches()
      // TODO more from heap arenas?
      SparkNettyMemoryHandle.getArenaMetrics(pool.directArenas().asScala, dest, offset + 5)
      dest(offset + allPooledMetrics.length - 1) =
        SparkNettyMemoryHandle.poolArenaFreeBytes(pool.heapArenas().asScala)
      offset += allPooledMetrics.length
    }
    // NOTE: netty 4.1 only, so spark 2.3+ only
    val unpooledMetric = UnpooledByteBufAllocator.DEFAULT.metric()
    dest(offset) = unpooledMetric.usedHeapMemory()
    dest(offset + 1) = unpooledMetric.usedDirectMemory()

  }
}

object SparkNettyMemoryHandle {

  def get(displayError: Boolean = false): Option[SparkNettyMemoryHandle] = try {
    val env = SparkEnv.get
    Some(new SparkNettyMemoryHandle(
      getRpcClientPooledAllocator(env).metric,
      getRpcServerPooledAllocator(env).metric,
      getBlockTransferServiceClientPooledAllocator(env).metric,
      getBlockTransferServiceServerPooledAllocator(env).metric
    ))
  } catch {
    case ex: Exception =>
      if (displayError) {
        ex.printStackTrace()
      }
      None
  }

  def getBlockTransferServiceClientPooledAllocator(env: SparkEnv): PooledByteBufAllocator = {
    import Reflector._
    val ftory = env.blockManager.reflectField("blockTransferService").reflectField("clientFactory")
    getPooledAllocatorFromClientFactory(ftory)
  }

  def getBlockTransferServiceServerPooledAllocator(env: SparkEnv): PooledByteBufAllocator = {
    import Reflector._
    val server = env.blockManager.reflectField("blockTransferService").reflectField("server")
    getServerPooledAllocator(server)
  }

  def getRpcClientPooledAllocator(env: SparkEnv): PooledByteBufAllocator = {
    import Reflector._
    val ftory = env.reflectField("rpcEnv").reflectField("clientFactory")
    getPooledAllocatorFromClientFactory(ftory)
  }

  def getRpcServerPooledAllocator(env: SparkEnv): PooledByteBufAllocator = {
    import Reflector._
    val server = env.reflectField("rpcEnv").reflectField("server")
    getServerPooledAllocator(server)
  }

  def getPooledAllocatorFromClientFactory(clientFactory: Any): PooledByteBufAllocator = {
    assert(clientFactory.getClass().getSimpleName.endsWith("TransportClientFactory"))
    import Reflector._
    clientFactory.reflectField("pooledAllocator").asInstanceOf[PooledByteBufAllocator]
  }

  def getServerPooledAllocator(server: Any): PooledByteBufAllocator = {
    assert(server.getClass().getSimpleName.endsWith("TransportServer"))
    // this looks like the best way to get it even in old versions (nettyMetric is only in 2.3+)
    import Reflector._
    val serverOptions = server.reflectField("bootstrap").reflectField("options")
      .asInstanceOf[java.util.Map[ChannelOption[_], Object]]
    serverOptions.get(ChannelOption.ALLOCATOR).asInstanceOf[PooledByteBufAllocator]
  }

  def getArenaMetrics(arenaMetrics: Seq[PoolArenaMetric], dest: Array[Long], offset: Int): Unit = {
    SparkNettyMemoryHandle.metricMethods.zipWithIndex.foreach { case (metric, idx) =>
      var total = 0L
      if (metric.getReturnType() == classOf[Int]){
        arenaMetrics.foreach { arena => total += metric.invoke(arena).asInstanceOf[Int] }
      } else {
        arenaMetrics.foreach { arena => total += metric.invoke(arena).asInstanceOf[Long] }
      }
      dest(offset + idx) = total
    }

    dest(offset + metricMethods.size) = poolArenaFreeBytes(arenaMetrics)

  }

  val VERBOSE_METRICS = Seq(
    "numAllocations",
    "numTinyAllocations",
    "numSmallAllocations",
    "numNormalAllocations",
    "numHugeAllocations",
    "numDeallocations",
    "numTinyDeallocations",
    "numSmallDeallocations",
    "numNormalDeallocations",
    "numHugeDeallocations",
    "numActiveAllocations",
    "numActiveTinyAllocations",
    "numActiveSmallAllocations",
    "numActiveNormalAllocations",
    "numActiveHugeAllocations"
  ).map((_, IncrementCounts)) ++ Seq(("numActiveBytes", IncrementBytes))

  val metricMethods = VERBOSE_METRICS.flatMap { case (methodName, _) =>
    val m = classOf[PoolArenaMetric].getMethod(methodName)
    if (Modifier.isPublic(m.getModifiers())) {
      Some(m)
    } else {
      None
    }
  }

  def poolArenaFreeBytes(arenas: Seq[PoolArenaMetric]): Long = {
    var total = 0L
    for {
      arena <- arenas
      list <- arena.chunkLists.asScala
      metric <- list.asScala
    } {
      total += metric.freeBytes()
    }
    total
  }

}
