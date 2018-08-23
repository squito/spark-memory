// (c) Copyright 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.spark

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
    delta > 1e7.toInt && (update.toDouble / orig) > 1.05
  }
}

case object IncrementCounts extends PeakReporting {
  override def report(orig: Long, update: Long): Boolean = {
    val delta = update - orig
    delta > 100 && (update.toDouble / orig) > 1.05
  }
}
