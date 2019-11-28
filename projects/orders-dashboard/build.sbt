name := "BlackFridayDashboard"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"
libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.8.1"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.10" % "provided"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"

// https://mvnrepository.com/artifact/org.wartremover/wartremover
addCompilerPlugin("org.wartremover" %% "wartremover" % "2.4.3" cross CrossVersion.full)
scalacOptions += "-P:wartremover:traverser:org.wartremover.warts.Unsafe"

// https://mvnrepository.com/artifact/com.sksamuel.scapegoat/scalac-scapegoat-plugin
scapegoatVersion in ThisBuild := "1.3.8"

