Project.inConfig(Test)(sbtprotoc.ProtocPlugin.protobufConfigSettings)

lazy val commonSettings = Seq(
  scalaVersion := "2.11.12",

  organization := "com.harborx",

  crossScalaVersions := Seq("2.11.12", "2.12.4"),

  scalacOptions := Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-encoding",
    "utf8",
    "-language:postfixOps",
    "-Yrangepos",
    "-language:implicitConversions"
  ),

  fork in Test := true,

  // protobuf
  PB.targets in Test := Seq(
    scalapb.gen(
      flatPackage = true,
      javaConversions = false,
      grpc = false,
      singleLineToString = true) -> (sourceManaged in Test).value
  )
)

// root project
lazy val mongo = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    // other settings
    name := "harborx-mongo",
    version := "1.0.6",
    libraryDependencies ++= Seq(
      // scalapb
      "com.trueaccord.scalapb" %% "scalapb-json4s" % "0.3.2",
      // mongo-scala-driver
      "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0",
      // Rx
      "org.reactivestreams" % "reactive-streams" % "1.0.1",
      // config
      "com.typesafe" % "config" % "1.3.0",
      // scalatest
      "org.scalatest" %% "scalatest" % "3.0.2" % "test",
      // akka stream for testing
      "com.typesafe.akka" %% "akka-stream" % "2.5.4" % "test",
      "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.4" % "test"
    )
  )
