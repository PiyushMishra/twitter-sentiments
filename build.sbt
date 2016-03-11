name := "TwitterWs"

version := "1.0"

lazy val twitterws = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.6"

libraryDependencies ++= Seq( jdbc , anorm , cache , ws )

libraryDependencies += "commons-httpclient" % "commons-httpclient" % "3.1"

libraryDependencies += "org.json4s" % "json4s-jackson_2.11" % "3.3.0"

libraryDependencies += "commons-io" % "commons-io" % "2.4"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.0-M14"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"

libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.1"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.4.2"

libraryDependencies += "com.typesafe.akka" % "akka-remote_2.11" % "2.4.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.4.1"

libraryDependencies += ("org.elasticsearch" % "elasticsearch-spark_2.11" % "2.1.0.Beta4")

libraryDependencies += "org.ahocorasick" % "ahocorasick" % "0.3.0"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1" classifier "models"

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"


unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )  