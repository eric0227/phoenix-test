import sbt.{ExclusionRule, StdoutOutput}

name := "phoenix-test"
version := "0.1"
scalaVersion := "2.11.8"


val sparkVersion = "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"
libraryDependencies += "org.apache.phoenix" % "phoenix-core" % "4.13.1-HBase-1.1"
libraryDependencies += ("org.apache.phoenix" % "phoenix-spark" % "4.14.0-HBase-1.2")
  //.excludeAll(ExclusionRule(organization = "org.apache.spark"))

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case PathList("com", "sun", xs@_*) => MergeStrategy.last
  case PathList("javax", "el", xs@_*) => MergeStrategy.last
  case PathList("javax", "ws", xs@_*) => MergeStrategy.last
  case PathList("org", "slf4j", xs@_*) => MergeStrategy.last
  case PathList("com", "fasterxml", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "overview.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "application.conf" => MergeStrategy.last
  case "logback.xml"      => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

packageBin in Compile := file(s"${name.value}_${scalaBinaryVersion.value}.jar")
