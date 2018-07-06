Spark Memory Monitor
========================

Usage
-----------

Build with `mvn package`, `sbt`, etc.

Include that jar in your spark application.  You could bundle it directly, or just include it with `--jars`.

The monitoring is configured via java system properties:

* "memory.monitor.enabled=true" -- if its set, enable monitoring (regardless of value)
* "memory.monitor.freq=[millis]" -- set the frequency of polling used to detect peaks, in millis

So a typical invocation might look like:

```
spark-submit \
  ...
  --jars spark-memory-core_2.11-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
  --driver-java-options "-XX:MaxMetaspaceSize=200M -XX:+PrintGCDetails -Dmemory.monitor.enabled=true -Dmemory.monitor.freq=100" \
  --conf spark.executor.extraJavaOptions="-XX:MaxMetaspaceSize=200M -XX:+PrintGCDetails -Dmemory.monitor.enabled=true -Dmemory.monitor.freq=100" \
  --conf spark.executor.plugins="com.cloudera.spark.MemoryMonitorExecutorExtension" \
  ...
```

This includes the Dynamic Allocation "plugin" (see below) -- note that requires a patched spark and the api may change.

You *also* have to modify the code of your job itself to turn it on.  Below are code samples (using reflection so your
build doesn't need to know about the memory monitor at all).


Installing on Driver
-----------------------

```scala
sys.props.get("memory.monitor.enabled").foreach { _ =>
  val clz = Class.forName("com.cloudera.spark.MemoryMonitor")
  val m = clz.getMethod("installIfSysProps")
  m.invoke(null)
}
```


Installing on Executors, with Static Allocation
-------------------------------------------------

Be sure to include this conf:

```scala
--conf "spark.scheduler.minRegisteredResourcesRatio=1.0"
```

and then in your job:

```scala
sys.props.get("memory.monitor.enabled").foreach { _ =>
  val clz = Class.forName("com.cloudera.spark.MemoryMonitor")
  val m = clz.getMethod("installOnExecIfStaticAllocation", classOf[org.apache.spark.SparkContext])
  m.invoke(null, sc)
}
```

Installing on Executors, with DynamicAllocation, and "Plugin"
-------------------------------------------------------------

Its trickier to get the MemoryMonitor installed on the executors with DynamicAllocation.  Executors can come and go at any time,
and we don't have a good way to initialize something before the first task is run.

You can modify spark to expose some sort of "executor-plugin api", like [this](https://github.com/squito/spark/commit/0ca94828e88006d2efeed6d106f4f0495cf3f5ee).
(Hopefully this can be exposed as part of spark eventually, I'll open a jira once I think about the api a little more.)
Then you'd reference the MemoryMonitor as an extension class:

```
--conf spark.executor.plugins="com.cloudera.spark.MemoryMonitorExecutorExtension"
```

Understanding the Output
=========================

The MemoryMonitor will poll the memory usage of a variety of subsystems used by Spark.  It tracks the memory of the JVM itself,
as well as offheap memory which is untracked by the JVM.  In addition it will report all updates to _peak_ memory use of each
subsystem, and log just the peaks.  This helps reduce the overhead and also make it manageable to view the logs.  Finally, it
registers a shutdown hook, to report both the current usage of each memory metric, and also the peak since it started monitoring.

The report is currently a grab-bag of many different metrics, but the most important are probably:

* `offheap:committed` -- in Java8, the memory used for code (aka the metaspace, though the JVM uses that term for
something slightly more specific)
* `netty-[subsystem]-numActiveBytes` -- bytes that netty has allocated in its offheap memory pools
* `netty-[subsystem]-directAllocatedUnused` -- bytes that netty has allocated in its offheap memory pools that are currently *unused*
* `netty-[subsystem]-heapAllocatedUnused` -- bytes that netty has allocated in its heap memory pools that are currently *unused*
* `on/offHeapStorage` -- bytes used by spark's block storage 
* `on/offHeapExecution` -- bytes used by spark's execution layer
* `on/offHeapMaxTaskExecution` -- the maximum number of "execution" bytes spark has assigned to any single task

Eg., will see sections of the stdout which just contain log lines of the form:

```
Peak Memory updates:Mon Jun 25 10:51:07 PDT 2018
offheap:committed       :       94.0 MB (+448.0 KB)
Code Cachecommitted     :       28.4 MB (+192.0 KB)
Metaspacecommitted      :       58.0 MB (+256.0 KB)
```

which show when the JVM is busy loading classes (perhaps even spark codegen classes)

Then when there is a burst of messages that have to be handled, you may see messages more like this:

```
netty-blockTransfer-server-usedHeapMem  :       16.0 MB (+16.0 MB)
netty-blockTransfer-server-usedDirectMem        :       16.0 MB (+16.0 MB)
netty-blockTransfer-server-numThreadLocalCaches :       8.0 B (+4.0 B)
netty-blockTransfer-server-numActiveBytes       :       16.0 MB (+16.0 MB)
netty-blockTransfer-server-directAllocatedUnused        :       15.9 MB (+15.9 MB)
netty-blockTransfer-server-heapAllocationUnused :       15.9 MB (+15.9 MB)
```

Note that polling is  imprecise.  The memory usage can spike very quickly.  You can increase the polling frequency, though this
may have adverse effects on performance.  (Currently we have not evaluated the performance impact **AT ALL**, so you're on your
own).

You can get around this limitation a *little* bit with the shutdown hook.  Hopefully, the memory usage in the shutdown hook might
give you some indication of the final memory usage -- especially when spark is killed by the OS / cluster manager.  However, 
shutdown hooks are best effort; they may not run, and even when they do, the JVM will be performing cleanup concurrently so you
might be observing memory in a different state.

In any case, the shut down metrics can be very valuable -- they look something like this:

```
IN SHUTDOWN
Mem usage at Mon Jun 25 11:03:51 PDT 2018
===============
heap:used       :1498.4 MB(1571212240)
heap:committed  :1902.0 MB(1994391552)
...

Peak Memory usage so far Mon Jun 25 11:03:51 PDT 2018
heap:used       :1257.2 MB                              Mon Jun 25 10:53:14 PDT 2018
heap:committed  :1999.0 MB                              Mon Jun 25 10:51:34 PDT 2018
offheap:used    :162.4 MB                               Mon Jun 25 10:51:31 PDT 2018
...
```

TODO
=======

* understand the performance impact
* more things to monitor -- eg. parquet?  is that covered by jvm metrics?
* spark plugin api, so we can use this w/ dynamic allocation
