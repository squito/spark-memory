// (c) Copyright 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.spark

class MemorySnapshot(
  val time: Long,
  val values: Array[Long]
)

class MemoryPeaks(
    val values: Array[Long],
    val peakTimes: Array[Long]) {
  def this(n:Int) {
    this(new Array[Long](n), new Array[Long](n))
  }

  def update(
      snapshot: MemorySnapshot,
      updates: PeakUpdate,
      reporting: IndexedSeq[PeakReporting]): Boolean = {
    assert(snapshot.values.length == values.length)
    var nUpdates = 0
    (0 until values.length).foreach { idx =>
      val orig = values(idx)
      val update = snapshot.values(idx)
      if (reporting(idx).report(orig, update)) {
        values(idx) = snapshot.values(idx)
        peakTimes(idx) = snapshot.time
        updates.updateIdx(nUpdates) = idx
        updates.delta(nUpdates) = update - orig
        nUpdates += 1
      }
    }
    updates.nUpdates = nUpdates
    nUpdates != 0
  }
}

class PeakUpdate(
  val updateIdx: Array[Int],
  val delta: Array[Long],
  var nUpdates: Int
) {
  def this(n: Int) {
    this(new Array[Int](n), new Array[Long](n), 0)
  }
}
