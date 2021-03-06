name := "logExcavator"

version := "0.0.1"

scalaVersion := "2.10.4"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.4.0" % "provided"

libraryDependencies += "com.github.scopt" %% "scopt" % "3.3.0" 

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.2"

resolvers += Resolver.sonatypeRepo("public")
