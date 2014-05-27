#!/bin/bash

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi
	
STORM_HOME=/home/pranab/Tools/storm-0.8.2
HDFS_BASE_DIR=/user/pranab/reco
PROP_FILE=/home/pranab/Projects/bin/sifarish/rt_reco.properties

case "$1" in

"copyPopular")
	echo "copy popular items to local FS"
	hadoop fs -get $HDFS_BASE_DIR/popu/part-r-00000 $2
	ls -l ~/Projects/bin/sifarish/$2
    ;;

"loadPopular")  
	echo  "loading popular items and ratings to redis cache"
	./engage_event.py loadPopRating $2
    ;;

"genEvents")  
	echo  "generating dither event and push to redis queue"
	./engage_event.py genPopDitherEvents $2 
    ;;

"showDitheredRecocache")  
	echo  "reading dithered recommendations from redis cache"
	./engage_event.py showDitheredRecoCache
    ;;

"startStorm")
	echo  "starting storm"
	$STORM_HOME/bin/storm nimbus &
	sleep 5
	$STORM_HOME/bin/storm supervisor & 
	sleep 5
	$STORM_HOME/bin/storm ui &
	;;

"buildJar")
	echo  "building uber jar"
	ant -f build_storm.xml
	;;

"deployTopology")
	echo  "deploying sifarish storm topology"
	$STORM_HOME/bin/storm  jar uber-sifarish-1.0.jar  org.sifarish.realtime.DitheringTopology  diReco rt_reco.properties
	;;

*) 
	echo "unknown operation $1"
	;;

esac
