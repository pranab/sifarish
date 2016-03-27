#!/bin/bash
JAR_NAME=/home/pranab/Projects/sifarish/target/sifarish-1.0.jar

case "$1" in

"diffTypeSim")
	echo "running MR for similarities between entities of different type"
	CLASS_NAME=org.sifarish.feature.DiffTypeSimilarity
	IN_PATH=/user/pranab/kiva/input
	OUT_PATH=/user/pranab/kiva/dts
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/sifarish/kiva.properties  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $OUT_PATH/_logs
	hadoop fs -rmr $OUT_PATH/_SUCCESS
	hadoop fs -ls $OUT_PATH
    ;;

"topMatches")
	echo "running MR for top matches based on nearest neighbors"
	CLASS_NAME=org.sifarish.feature.TopMatches
	IN_PATH=/user/pranab/kiva/dts
	OUT_PATH=/user/pranab/kiva/tom
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/sifarish/kiva.properties  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $OUT_PATH/_logs
	hadoop fs -rmr $OUT_PATH/_SUCCESS
	hadoop fs -ls $OUT_PATH
    ;;

*) 
	echo "unknown operation $1"
	;;

esac

