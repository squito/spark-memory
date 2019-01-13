// (c) Copyright 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.spark

import java.lang.reflect.Modifier

import scala.collection.JavaConverters._

import io.netty.buffer._
import io.netty.channel.ChannelOption

import org.apache.spark.SparkEnv

import scala.util.Try

class SparkNettyMemoryHandle(
  val rpcClientPool: PooledByteBufAllocatorMetric,
  val rpcServerPool: Option[PooledByteBufAllocatorMetric],
  val blockClientPool: PooledByteBufAllocatorMetric,
  val blockServerPool: PooledByteBufAllocatorMetric,
  val externalShuffleClientPool: Option[PooledByteBufAllocatorMetric]
) extends MemoryGetter {
  override def toString(): String = {
    "RPC Client pool:" + rpcClientPool + "\n" +
      "RPC Server pool:" + rpcServerPool + "\n" +
      "Block Transfer client pool:" + blockClientPool + "\n" +
      "Block Transfer server pool:" + blockServerPool + "\n" +
      "External Shuffle Client pool:" + externalShuffleClientPool
  }

  val poolMetrics = Seq(
    ("usedHeapMem", IncrementBytes),
    ("usedDirectMem", IncrementBytes),
    ("numHeapArenas", AllIncrements),
    ("numDirectArenas", AllIncrements),
    ("numThreadLocalCaches", AllIncrements))

  val allPooledMetrics = poolMetrics ++ SparkNettyMemoryHandle.VERBOSE_METRICS ++
    Seq(("directAllocatedUnused", IncrementBytes), ("heapAllocationUnused", IncrementBytes))

  val poolsAndNames: Seq[(PooledByteBufAllocatorMetric, String)] = Seq(
    Some(rpcClientPool, "rpc-client"),
    rpcServerPool.map((_, "rpc-server")),
    Some(blockClientPool, "blockTransfer-client"),
    Some(blockServerPool, "blockTransfer-server"),
    externalShuffleClientPool.map((_, "external-shuffle-client"))
  ).flatten

  val pools = poolsAndNames.map(_._1)

  override val namesAndReporting: Seq[(String, PeakReporting)] = (for {
    (_, poolName) <- poolsAndNames
    (metric, reporting) <- allPooledMetrics
  } yield {
      ("netty-" + poolName + "-" + metric, reporting)
  }) ++ Seq(
    ("netty-Unpooled-heapUsed", IncrementBytes),
    ("netty-Unpooled-directUsed", IncrementBytes)
  )

  def values(dest: Array[Long], initOffset: Int): Unit = {
    var offset = initOffset
    pools.foreach { pool =>
      dest(offset) = pool.usedHeapMemory()
      dest(offset + 1) = pool.usedDirectMemory()
      dest(offset + 2) = pool.numHeapArenas()
      dest(offset + 3) = pool.numDirectArenas()
      dest(offset + 4) = pool.numThreadLocalCaches()
      // get active and unused bytes from the direct arenas
      SparkNettyMemoryHandle.getArenaMetrics(pool.directArenas().asScala, dest, offset + 5)
      // from the onheap arenas, just get the unused bytes (this arena isn't as important, so not
      // bothering with active bytes count)
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

  def get(displayError: Boolean = false): Option[SparkNettyMemoryHandle] = {
    Option(SparkEnv.get).map { env =>
      new SparkNettyMemoryHandle(
        getRpcClientPooledAllocator(env).metric,
        Try(getRpcServerPooledAllocator(env).metric).toOption,
        getBlockTransferServiceClientPooledAllocator(env).metric,
        getBlockTransferServiceServerPooledAllocator(env).metric,
        getExternalShuffleServiceClientPooledAllocator(env).map(_.metric)
      )
    }
  }

  def getBlockTransferServiceClientPooledAllocator(env: SparkEnv): PooledByteBufAllocator = {
    import Reflector._
    val ftory = env.blockManager.reflectField("blockTransferService").reflectField("clientFactory")
    getPooledAllocatorFromClientFactory(ftory)
  }

  def getExternalShuffleServiceClientPooledAllocator(env: SparkEnv): Option[PooledByteBufAllocator] = {
    import Reflector._
    val shuffleClient = env.blockManager.reflectField("shuffleClient")
    println(s"shuffleClient = $shuffleClient (${shuffleClient.getClass()})")
    if (shuffleClient.getClass().getSimpleName.endsWith("ExternalShuffleClient")) {
      Some(getPooledAllocatorFromClientFactory(shuffleClient.reflectField("clientFactory")))
    } else {
      None
    }
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

  val VERBOSE_METRICS = Seq(("numActiveBytes", IncrementBytes))

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
