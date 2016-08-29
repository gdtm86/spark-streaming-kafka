name := "transamerica_spark_streaming_kafka"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.6.0-cdh5.5.4" % "provided" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" %% "spark-streaming" % "1.5.0-cdh5.5.4" % "provided" ,
  "org.apache.spark" %% "spark-core" % "1.5.0-cdh5.5.4" % "provided" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" %% "spark-streaming-twitter" % "1.5.0-cdh5.5.4",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.5.0-cdh5.5.4",
  "org.apache.spark" %% "spark-sql" % "1.5.0-cdh5.5.4" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.5.0-cdh5.5.4" % "provided",
  "org.apache.hbase" % "hbase-client" % "1.0.0-cdh5.5.4" % "provided",
  "org.apache.hbase" % "hbase-common" % "1.0.0-cdh5.5.4" % "provided",
  "com.google.code.gson" % "gson" % "1.7.1"
)


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Maven Central Server" at "http://repo1.maven.org/maven2",
  "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
)

