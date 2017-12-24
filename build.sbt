resolvers in ThisBuild ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal
)

name := "transaction-log-consumer"

version := "0.1"

organization := "com.syarif.ao"

scalaVersion in ThisBuild := "2.11.11"

val protobufVersion = "3.1.0"

val flinkVersion = "1.3.2"

val flinkDependencies = Seq(
  "org.apache.flink" % "flink-jdbc" % flinkVersion,
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-connector-kafka-0.10" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-table" % flinkVersion,
  "org.apache.flink" %% "flink-connector-filesystem" % flinkVersion
)

val jobDependencies = Seq(
  "com.google.protobuf" % "protobuf-java" % protobufVersion,
  "com.google.protobuf" % "protobuf-java-util" % protobufVersion,
)


lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies ++ jobDependencies
  )


mainClass in assembly := Some("com.syarif.ao.LogConsumer")

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
                                   mainClass in (Compile, run),
                                   runner in (Compile,run)
                                  ).evaluated

fork in run := true
javaOptions in run += "-Duser.timezone=UTC"

assemblyMergeStrategy in assembly := {
  case PathList("com", "google", "protobuf", xs @ _*) => MergeStrategy.first
  case PathList("google", "protobuf", xs @ _*) => MergeStrategy.first
  case PathList("com", "google", "cloud","google-cloud-core", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


