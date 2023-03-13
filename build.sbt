name := "Data Challenge"

version := "1.0"
scalaVersion := "2.13.10"
scalacOptions ++= Seq( // use ++= to add to existing options
  "-encoding",
  "utf8", // if an option takes an arg, supply it on the same line
  "-feature", // then put the next option on a new line for easy editing
  "-language:implicitConversions",
  "-language:existentials",
  "-unchecked",
  "-Werror",
  "-deprecation",
  "-Xlint" // exploit "trailing comma" syntax so you can add an option without editing this line
)
javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.2"
run / fork := true