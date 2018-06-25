Spark Memory Monitor
========================

Usage
-----------

Build with `mvn package`, `sbt`, etc.

Include that jar in your spark application.  You could bundle it directly, or just include it with `--jars`.

The monitoring is configured via java system properties:

* "memory.monitor.enabled" -- if its set, enable monitoring (regardless of value)
* "memory.monitor.freq=<millis>" -- set the frequency of polling used to detect peaks, in millis

So a typical invocation might look like:

```
spark-submit \
 ...
 --jars spark-memory-core_2.11-0.1.0-SNAPSHOT-jar-with-dependencies.jar \
 --driver-java-options "-XX:MaxMetaspaceSize=200M -XX:+PrintGCDetails -Dmemory.monitor.enabled -Dmemory.monitor.freq=100" \
 --conf spark.executor.extraJavaOptions="-XX:MaxMetaspaceSize=200M -XX:+PrintGCDetails -Dmemory.monitor.enabled -Dmemory.monitor.freq=100" \
 ...
```

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
