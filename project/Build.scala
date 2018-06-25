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

import java.io._

import scala.collection.JavaConversions._
import scala.util.Properties

import sbt._
import sbt.Classpaths.publishTask
import sbt.Keys._
import sbtunidoc.Plugin.UnidocKeys.unidocGenjavadocVersion
import com.typesafe.sbt.pom.{loadEffectivePom, PomBuild, SbtPomKeys}
import net.virtualvoid.sbt.graph.Plugin.graphSettings

import spray.revolver.RevolverPlugin._

object BuildCommons {

  private val buildLocation = file(".").getAbsoluteFile.getParentFile
  val testTempDir = buildLocation + "/target/tmp"

  val allProjects @ Seq(core, examples) =
    Seq("core", "examples").map(ProjectRef(buildLocation, _))
  val assemblyProjects @ Seq(assembly) =
    Seq("assembly").map(ProjectRef(buildLocation, _))

}

object MyBuild extends PomBuild {

  import BuildCommons._
  import scala.collection.mutable.Map

  val projectsMap: Map[String, Seq[Setting[_]]] = Map.empty

  override val userPropertiesMap = System.getProperties.toMap

  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val publishLocalBoth = TaskKey[Unit]("publish-local", "publish local for m2 and ivy")

  lazy val sharedSettings = graphSettings ++ Seq (
    javaHome := sys.env.get("JAVA_HOME")
      .orElse(sys.props.get("java.home").map { p => new File(p).getParentFile().getAbsolutePath() })
      .map(file),
    incOptions := incOptions.value.withNameHashing(true),
    retrieveManaged := true,
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    publishMavenStyle := true,

    resolvers += Resolver.mavenLocal,
    otherResolvers <<= SbtPomKeys.mvnLocalRepository(dotM2 => Seq(Resolver.file("dotM2", dotM2))),
    publishLocalConfiguration in MavenCompile <<= (packagedArtifacts, deliverLocal, ivyLoggingLevel) map {
      (arts, _, level) => new PublishConfiguration(None, "dotM2", arts, Seq(), level)
    },
    publishMavenStyle in MavenCompile := true,
    publishLocal in MavenCompile <<= publishTask(publishLocalConfiguration in MavenCompile, deliverLocal),
    publishLocalBoth <<= Seq(publishLocal in MavenCompile, publishLocal).dependOn,

    javacOptions in (Compile, doc) ++= {
      val Array(major, minor, _) = System.getProperty("java.version").split("\\.", 3)
      if (major.toInt >= 1 && minor.toInt >= 8) Seq("-Xdoclint:all", "-Xdoclint:-missing") else Seq.empty
    },

    javacOptions in Compile ++= Seq("-encoding", "UTF-8", "-source", "1.8", "-target", "1.8"),

    // this lets us run spark apps from within sbt, but still leave it as a "provided" dependency, so its
    // not bundled into jars.  See http://stackoverflow.com/questions/18838944/how-to-add-provided-dependencies-back-to-run-test-tasks-classpath
    runMain in Compile <<= Defaults.runMainTask(fullClasspath in Compile, runner in (Compile, run))
  )

  def enable(settings: Seq[Setting[_]])(projectRef: ProjectRef) = {
    val existingSettings = projectsMap.getOrElse(projectRef.project, Seq[Setting[_]]())
    projectsMap += (projectRef.project -> (existingSettings ++ settings))
  }

  // Note ordering of these settings matter.
  /* Enable shared settings on all projects */
  (allProjects ++ assemblyProjects)
    .foreach(enable(sharedSettings ++ Revolver.settings))

  /* Enable tests settings for all projects */
  allProjects.foreach(enable(TestSettings.settings))

  /* Enable Assembly for all projects */
  enable(Assembly.settings)(core)

  // TODO: move this to its upstream project.
  override def projectDefinitions(baseDirectory: File): Seq[Project] = {
    super.projectDefinitions(baseDirectory).map { x =>
      if (projectsMap.exists(_._1 == x.id)) x.settings(projectsMap(x.id): _*)
      else x.settings(Seq[Setting[_]](): _*)
    }
  }

}

object Assembly {
  import sbtassembly.AssemblyUtils._
  import sbtassembly.Plugin._
  import AssemblyKeys._

  val hadoopVersion = taskKey[String]("The version of hadoop that spark is compiled against.")

  lazy val settings = assemblySettings ++ Seq(
    test in assembly := {},
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    mergeStrategy in assembly := {
      case PathList("org", "datanucleus", xs @ _*)             => MergeStrategy.discard
      case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf")      => MergeStrategy.discard
      case "log4j.properties"                                  => MergeStrategy.discard
      case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
      case "reference.conf"                                    => MergeStrategy.concat
      case _                                                   => MergeStrategy.first
    }
  )
}

object TestSettings {
  import BuildCommons._

  lazy val settings = Seq (
    // Fork new JVMs for tests and set Java options for those
    fork := true,
    // Setting SPARK_DIST_CLASSPATH is a simple way to make sure any child processes
    // launched by the tests have access to the correct test-time classpath.
    envVars in Test ++= Map(
      "SPARK_DIST_CLASSPATH" ->
        (fullClasspath in Test).value.files.map(_.getAbsolutePath).mkString(":").stripSuffix(":"),
      "JAVA_HOME" -> sys.env.get("JAVA_HOME").getOrElse(sys.props("java.home"))),
    javaOptions in Test += s"-Djava.io.tmpdir=" + testTempDir,
    javaOptions in Test += "-Dspark.port.maxRetries=100",
    javaOptions in Test += "-Dspark.ui.enabled=false",
    javaOptions in Test += "-Dspark.ui.showConsoleProgress=false",
    javaOptions in Test += "-Dspark.driver.allowMultipleContexts=true",
    javaOptions in Test += "-Dspark.unsafe.exceptionOnMemoryLeak=true",
    javaOptions in Test += "-Dsun.io.serialization.extendedDebugInfo=true",
    javaOptions in Test += "-Dderby.system.durability=test",
    javaOptions in Test ++= System.getProperties.filter(_._1 startsWith "spark")
      .map { case (k,v) => "-D" + k + "=" + v }.toSeq,
    javaOptions in Test += "-ea",
    javaOptions in Test ++= "-Xmx3g -Xss4096k -XX:PermSize=128M -XX:MaxNewSize=256m -XX:MaxPermSize=1g"
      .split(" ").toSeq,
    javaOptions += "-Xmx3g",
    // Show full stack trace and duration in test cases.
    testOptions in Test += Tests.Argument("-oDF"),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
    // Enable Junit testing.
    libraryDependencies += "com.novocode" % "junit-interface" % "0.9" % "test",
    // Only allow one test at a time, even across projects, since they run in the same JVM
    parallelExecution in Test := false,
    // Make sure the test temp directory exists.
    resourceGenerators in Test <+= resourceManaged in Test map { outDir: File =>
      if (!new File(testTempDir).isDirectory()) {
        require(new File(testTempDir).mkdirs())
      }
      Seq[File]()
    },
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
    // Remove certain packages from Scaladoc
    scalacOptions in (Compile, doc) := Seq(
      "-groups",
      "-skip-packages", Seq(
        "akka",
        "org.apache.spark.api.python",
        "org.apache.spark.network",
        "org.apache.spark.deploy",
        "org.apache.spark.util.collection"
      ).mkString(":"),
      "-doc-title", "Spark " + version.value.replaceAll("-SNAPSHOT", "") + " ScalaDoc"
    )
  )

}
