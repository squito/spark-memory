Spark Memory Monitor
========================


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
