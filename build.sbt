
name := "scala-riak-client"

organization := "com.whitepages"

scalaVersion := "2.11.1"

crossScalaVersions := Seq("2.11.1")

javaOptions ++= Seq("-Djava.net.preferIPv4Stack=true")

scalacOptions in (Compile, doc) ++= Seq("-skip-packages", "antlr")

libraryDependencies ++= Seq( "com.whitepages" %% "scala-webservice" % "9.4.10-SNAPSHOT"
                           , "com.basho.riak" % "riak-client" % "2.0.0"
                           )
