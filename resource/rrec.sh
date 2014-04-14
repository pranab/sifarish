#!/bin/bash

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi
	
JAR_NAME=/home/pranab/Projects/sifarish/target/sifarish-1.0.jar
STORM_HOME=/home/pranab/Tools/storm-0.8.2

case "$1" in
"expSchema")  
	echo "exporting event mapping metadata to HDFS"
	hadoop fs -rmr /user/pranab/meta/real/*
	hadoop fs -put engageEvent.json /user/pranab/meta/real
	hadoop fs -ls /user/pranab/meta/real
    ;;
 
"genHistEvent")  
	echo "generating historical event data"
	ruby ratings.rb $2 $3 $4
    ;;
 
"expEvent")  
	echo "exporting event data to HDFS"
	hadoop fs -rmr /user/pranab/real/enga/*
	hadoop fs -put $2 /user/pranab/real/enga
	hadoop fs -ls /user/pranab/real/enga
    ;;

"genRating")  
	echo "running MR to generate implicit rating from event data"
	CLASS_NAME=org.sifarish.common.ImplicitRatingEstimator
	IN_PATH=/user/pranab/real/enga
	OUT_PATH=/user/pranab/real/rate
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/sifarish/rt_reco.properties  $IN_PATH  $OUT_PATH
	hadoop fs -rmr "/user/pranab/real/rate/_logs"
	hadoop fs -rmr "/user/pranab/real/rate/_SUCCESS"
	hadoop fs -ls /user/pranab/real/rate
    ;;

"compactRating")  
	echo "running MR to format rating to compact form"
	CLASS_NAME=org.sifarish.common.CompactRatingFormatter
	IN_PATH=/user/pranab/real/rate
	OUT_PATH=/user/pranab/real/crat
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/sifarish/rt_reco.properties  $IN_PATH  $OUT_PATH
	hadoop fs -rmr "/user/pranab/real/crat/_logs"
	hadoop fs -rmr "/user/pranab/real/crat/_SUCCESS"
	hadoop fs -ls /user/pranab/real/crat
    ;;

"correlation")  
	echo  "running MR to generate item correlation from rating data"
	CLASS_NAME=org.sifarish.common.ItemDynamicAttributeSimilarity
	IN_PATH=/user/pranab/real/crat
	OUT_PATH=/user/pranab/real/simi
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/sifarish/rt_reco.properties  $IN_PATH  $OUT_PATH
	hadoop fs -rmr "/user/pranab/real/simi/_logs"
	hadoop fs -rmr "/user/pranab/real/simi/_SUCCESS"
	hadoop fs -ls /user/pranab/real/simi
    ;;

"corrMatrix")  
	echo  "running MR to transform item correlation to matrix form"
	CLASS_NAME=org.sifarish.common.CorrelationMatrixBuilder
	IN_PATH=/user/pranab/real/simi
	OUT_PATH=/user/pranab/real/matr
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/sifarish/rt_reco.properties  $IN_PATH  $OUT_PATH
	hadoop fs -ls /user/pranab/real/matr
    ;;

"impCorrMatrix")  
	echo  "importing correlation matrix to loal FS"
	hadoop fs -get /user/pranab/real/matr/part-r-00000 $2
	ls -l ~/Projects/bin/sifarish/*.txt
    ;;

"loadCorrMatrix")  
	echo  "loading correlation to redis cache"
	./engage_event.py loadCorrelation $2
    ;;

"loadEventMapping")  
	echo  "loading event mapping meta data to redis cache"
	./engage_event.py loadEventMapping $2
    ;;

"genEvents")  
	echo  "generating events and pushing to redis queue"
	./engage_event.py genEvents $2 $3
    ;;
    
"showRecoQueue")  
	echo  "reading recommendations from redis queue"
	./engage_event.py showRecoQueue
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
	$STORM_HOME/bin/storm  jar uber-sifarish-1.0.jar  org.sifarish.realtime.RecommenderTopology  rtReco rt_reco.properties
	;;
	
*) 
	echo "unknown operation $1"
	;;

esac
