name := "GDELT-GKG Scala"

version := "0.1"

scalaVersion := "2.11.7"

resolvers += "Neo4j Scala Repo" at "http://m2.neo4j.org/content/repositories/releases"


libraryDependencies += "org.scala-lang" % "scala-xml" % "2.11.0-M4"

libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.7"

libraryDependencies += "org.mongodb" %% "casbah" % "3.1.1"

libraryDependencies += "eu.fakod" % "neo4j-scala_2.10" % "0.3.0"
