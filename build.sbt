
name := "receita-federal"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "9.4-1200-jdbc41",
  "mysql" % "mysql-connector-java" % "5.1.16",
  "com.google.cloud" % "google-cloud-storage" % "1.74.0",
  "org.apache.spark" %% "spark-core" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.1"  % "provided",
  "com.spotify" %% "spark-bigquery" % "0.2.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
  "com.typesafe" % "config" % "1.3.4",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.1",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.4.2",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "com.databricks" %% "spark-xml" % "0.4.1",
  "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.10.2-hadoop2"
    exclude ("com.google.guava", "guava-jdk5"),
  "com.crealytics" %% "spark-excel" % "0.8.2"
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "googlecommon.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

mainClass in assembly := Some("br.com.bruno.data.ingestion.receitafederal.Starter")

assemblyJarName in assembly := "receita_federal.jar"