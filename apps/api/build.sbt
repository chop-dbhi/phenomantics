import AssemblyKeys._

assemblySettings

name := "phenomantics_api"

organization := "edu.chop.cbmi"

version := "0.1"

scalaVersion := "2.10.1"

//note %% implies a check against scalaVersion
libraryDependencies ++= Seq(
	"org.scalatest" %% "scalatest" % "2.0.M5b" % "test",
	"com.typesafe"  %  "config"    % "1.0.0", //use the same version as akka see mvn rep
	"org.postgresql" % "postgresql" % "9.2-1003-jdbc4"
)

scalacOptions += "-deprecation"

scalacOptions += "-unchecked"

scalacOptions += "-feature"

//skip tests during assembly
test in assembly := {}

jarName in assembly := "phenomantics.jar"

//exclude Scala from assembly jar
assembleArtifact in packageScala := false
