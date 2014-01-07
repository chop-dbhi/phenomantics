import sbt._
import sbt.Keys._

object GenesisBuild extends Build {

  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.0.9"

  lazy val genesis = Project(
    id = "genesis",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "genesis",
      organization := "edu.chop.cbmi",
      version := "1.0",
      scalaVersion := "2.10.1",
      javaOptions += "Xmx1G",
      libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-actor" % "2.1.2",
	      "com.typesafe.akka" %% "akka-kernel" % "2.1.2",
        "com.typesafe.akka" %% "akka-remote" % "2.1.2",
        "com.typesafe.akka" %% "akka-slf4j" % "2.1.2",
        "ch.qos.logback" % "logback-classic" % "1.0.9",
        "javax.mail" % "mail" % "1.4.7",
        "javax.activation" % "activation" % "1.1",
        "org.apache.commons" % "commons-compress" % "1.5",
        "com.amazonaws" % "aws-java-sdk" % "1.4.6"
      )
    )
  )
}
