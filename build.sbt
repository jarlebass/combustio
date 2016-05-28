scalaVersion := "2.10.5"

val sparkV = "1.4.1"

def unitFilter(name: String): Boolean = !name.endsWith("IntegrationTest")

lazy val UnitTest = config("unit").extend(Test)

mergeStrategy in assembly := {
    case x: String if x.contains("org/slf4j/impl") => MergeStrategy.first
      case x =>
            val oldStrategy = (mergeStrategy in assembly).value
                oldStrategy(x)
}

lazy val root = (project in file("."))
  .settings(assemblyJarName in assembly := "app.jar")
  .settings(inConfig(UnitTest)(Defaults.testTasks): _*)
  .configs(UnitTest)
  .settings(testOptions in UnitTest := Seq(Tests.Filter(unitFilter)))
  .settings(
    libraryDependencies ++= Seq(
      "com.beust"                   %   "jcommander"            % "1.48",
      "ch.qos.logback"              %   "logback-classic"       % "1.1.2",
      "com.typesafe.scala-logging"  %%  "scala-logging-slf4j"   % "2.1.2",
      "net.liftweb"                 %%  "lift-json"             % "2.6.2",
      "org.apache.spark"            %%  "spark-core"            % sparkV      % "provided",
      "org.apache.spark"            %%  "spark-core"            % sparkV      % "test",
      "org.scalatest"               %%  "scalatest"             % "3.0.0-M7"  % "test"
    )
  )
