organization := "mawazo"

name := "sifarish"

version := "1.0"

scalaVersion := "2.10.4"

packageBin in Compile := file(s"target/${name.value}-${version.value}.jar")