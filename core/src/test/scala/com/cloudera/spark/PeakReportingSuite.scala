// (c) Copyright 2018 Cloudera, Inc. All rights reserved.
package com.cloudera.spark

import com.typesafe.scalalogging.Logger
import org.scalatest.FunSuite

class PeakReportingSuite extends FunSuite {

  private[this] val log = Logger(this.getClass)

  test("increment bytes") {

    log.info("================== test IncrementBytes.report start.")

    // delta over 1e7, and 5% increase
    assert(IncrementBytes.report(1e9.toLong, 1.051e9.toLong))
    // delta over 1e7, but less than 5% increase
    assert(!IncrementBytes.report(1e9.toLong, 1.049e9.toLong))

    //5% increase, but below overall threshold
    assert(!IncrementBytes.report(1e7.toLong, 1.05e7.toLong))
    assert(!IncrementBytes.report(1e7.toLong, 1.9e7.toLong))
    assert(!IncrementBytes.report(1e6.toLong, 1e7.toLong))

    // increase from small starting point OK
    assert(IncrementBytes.report(0, 1.001e7.toLong))

    log.info("================== test IncrementBytes.report end.")

  }
}
