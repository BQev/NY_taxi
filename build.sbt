name := "Taxi"

version := "0.1"

scalaVersion := "2.11.12"
val  sparkVersion ="2.4.8"

val sparkDependencies= Seq(
 "org.apache.spark" %% "spark-core" % sparkVersion,
 "org.apache.spark" %% "spark-sql" % sparkVersion
)


libraryDependencies++= sparkDependencies