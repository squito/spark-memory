/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.executor

import java.io._
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

case class MemoryUsage(
  val javaRSSTotal: Long,
  val javaVmemTotal: Long,
  val pythonRSSTotal: Long,
  val pythonVmemTotal: Long,
  val otherRSSTotal: Long,
  val otherVmemTotal: Long)

class ProcfsBasedMetrics extends Logging {

  val PROCFS_DIR = "/proc"
  var isAvailable: Boolean = isProcfsFound
  val pid: Int = computePid()
  val ptree: scala.collection.mutable.Map[Int, Set[Int]] =
    scala.collection.mutable.Map[Int, Set[Int]]()
  val PROCFS_STAT_FILE = "stat"
  var javaVmemTotal: Long = 0
  var javaRSSTotal: Long = 0
  var pythonVmemTotal: Long = 0
  var pythonRSSTotal: Long = 0
  var otherVmemTotal: Long = 0
  var otherRSSTotal: Long = 0

  createProcessTree()

  def isProcfsFound: Boolean = {
    try {
      if (!Files.exists(Paths.get(PROCFS_DIR))) {
        return false
      }
    } catch {
      case e: FileNotFoundException => return false
    }
    true
  }

  def computePid(): Int = {
    if (!isAvailable) {
      return -1;
    }
    try {
      // This can be simplified in java9:
      // https://docs.oracle.com/javase/9/docs/api/java/lang/ProcessHandle.html
      val cmd = Array("bash", "-c", "echo $PPID")
      val length = 10
      var out: Array[Byte] = Array.fill[Byte](length)(0)
      Runtime.getRuntime.exec(cmd).getInputStream.read(out)
      val pid = Integer.parseInt(new String(out, "UTF-8").trim)
      pid;
    } catch {
      case NonFatal(e) => logDebug("An error occurred when trying to compute the process tree. " +
          "As a result, reporting of process tree metrics is stopped.")
        isAvailable = false
        -1
    }
  }

  def createProcessTree(): Unit = {
    if (!isAvailable) {
      return
    }
    val queue: Queue[Int] = new Queue[Int]()
    queue += pid
    while (!queue.isEmpty) {
      val p = queue.dequeue()
      val c = getChildPIds(p)
      if (!c.isEmpty) {
        queue ++= c
        ptree += (p -> c.toSet)
      } else {
        ptree += (p -> Set[Int]())
      }
    }
  }

  def updateProcessTree(): Unit = {
    if (!isAvailable) {
      return
    }
    val queue: Queue[Int] = new Queue[Int]()
    queue += pid
    while (!queue.isEmpty) {
      val p = queue.dequeue()
      val c = getChildPIds(p)
      if (!c.isEmpty) {
        queue ++= c
        val preChildren = ptree.get(p)
        preChildren match {
          case Some(children) => if (!c.toSet.equals(children)) {
            val diff: Set[Int] = children -- c.toSet
            ptree.update(p, c.toSet )
            diff.foreach(ptree.remove(_))
          }
          case None => ptree.update(p, c.toSet )
        }
      } else {
        ptree.update(p, Set[Int]())
      }
    }
  }

  /**
   * The computation of RSS and Vmem is based on proc(5):
   * http://man7.org/linux/man-pages/man5/proc.5.html
   */
  def getProcessInfo(pid: Int): Unit = {
    try {
      val pidDir = new File(PROCFS_DIR, pid.toString)
      val statFile = new File(pidDir, PROCFS_STAT_FILE)
      val in = new BufferedReader(new InputStreamReader(
        new FileInputStream(statFile), Charset.forName("UTF-8")))
      val procInfo = in.readLine
      in.close
      val procInfoSplit = procInfo.split(" ")
      if (procInfoSplit != null) {
        if (procInfoSplit(1).toLowerCase.contains("java")) {
          javaVmemTotal += procInfoSplit(22).toLong
          javaRSSTotal += procInfoSplit(23).toLong
        } else if (procInfoSplit(1).toLowerCase.contains("python")) {
          pythonVmemTotal += procInfoSplit(22).toLong
          pythonRSSTotal += procInfoSplit(23).toLong
        } else {
          otherVmemTotal += procInfoSplit(22).toLong
          otherRSSTotal += procInfoSplit(23).toLong
        }
      }
    } catch {
      case e: FileNotFoundException =>
    }
  }

  def getMemoryUsage(): MemoryUsage = {
    if (!isAvailable) {
      return null
    }
    updateProcessTree()
    val pids = ptree.keySet
    javaRSSTotal = 0
    javaVmemTotal = 0
    pythonRSSTotal = 0
    pythonVmemTotal = 0
    otherRSSTotal = 0
    otherVmemTotal = 0
    for (p <- pids) {
      getProcessInfo(p)
    }
    MemoryUsage(
      javaRSSTotal,
      javaVmemTotal,
      pythonRSSTotal,
      pythonVmemTotal,
      otherRSSTotal,
      otherVmemTotal)
  }

  def getChildPIds(pid: Int): ArrayBuffer[Int] = {
    try {
      val cmd = Array("pgrep", "-P", pid.toString)
      val input = Runtime.getRuntime.exec(cmd).getInputStream
      val childPidsInByte: mutable.ArrayBuffer[Byte] = new mutable.ArrayBuffer()
      var d = input.read()
      while (d != -1) {
        childPidsInByte.append(d.asInstanceOf[Byte])
        d = input.read()
      }
      input.close()
      val childPids = new String(childPidsInByte.toArray, "UTF-8").split("\n")
      val childPidsInInt: ArrayBuffer[Int] = new ArrayBuffer[Int]()
      for (p <- childPids) {
        if (p != "") {
          childPidsInInt += Integer.parseInt(p)
        }
      }
      childPidsInInt
    } catch {
      case NonFatal(e) => logDebug("An error occurred when trying to compute the process tree. " +
          "As a result, reporting of process tree metrics is stopped.")
        isAvailable = false
        new mutable.ArrayBuffer()
    }
  }
}
