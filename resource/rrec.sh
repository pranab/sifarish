#!/bin/bash

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi
	
JAR_NAME=/home/pranab/Projects/sifarish/target/sifarish-1.0.jar
STORM_HOME=/home/pranab/Tools/storm-0.8.2
HDFS_BASE_DIR=/user/pranab/real
PROP_FILE=/home/pranab/Projects/bin/sifarish/rt_reco.properties

case "$1" in
"expSchema")  
	echo "exporting event mapping metadata to HDFS"
	hadoop fs -rmr /user/pranab/meta/real/*
	hadoop fs -put engageEvent.json /user/pranab/meta/real
	hadoop fs -ls /user/pranab/meta/real
    ;;
 
"genHistEvent")  
	echo "generating historical event data"
	./engage.rb $2 $3 $4
    ;;
 
"expEvent")  
	echo "exporting event data to HDFS"
	hadoop fs -rmr $HDFS_BASE_DIR/enga/*
	hadoop fs -put $2 $HDFS_BASE_DIR/enga
	hadoop fs -ls $HDFS_BASE_DIR/enga
    ;;

"genRating")  
	echo "running MR to generate implicit rating from event data"
	CLASS_NAME=org.sifarish.common.ImplicitRatingEstimator
	IN_PATH=$HDFS_BASE_DIR/enga
	OUT_PATH=$HDFS_BASE_DIR/rate
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr "$HDFS_BASE_DIR/rate/_logs"
	hadoop fs -rmr "$HDFS_BASE_DIR/rate/_SUCCESS"
	hadoop fs -ls $HDFS_BASE_DIR/rate
    ;;

"compactRating")  
	echo "running MR to format rating to compact form"
	CLASS_NAME=org.sifarish.common.CompactRatingFormatter
	IN_PATH=$HDFS_BASE_DIR/rate
	OUT_PATH=$HDFS_BASE_DIR/crat
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr "$HDFS_BASE_DIR/crat/_logs"
	hadoop fs -rmr "$HDFS_BASE_DIR/crat/_SUCCESS"
	hadoop fs -ls $HDFS_BASE_DIR/crat
    ;;

"correlation")  
	echo  "running MR to generate item correlation from rating data"
	CLASS_NAME=org.sifarish.common.ItemDynamicAttributeSimilarity
	IN_PATH=$HDFS_BASE_DIR/crat
	OUT_PATH=$HDFS_BASE_DIR/simi
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr "$HDFS_BASE_DIR/simi/_logs"
	hadoop fs -rmr "$HDFS_BASE_DIR/simi/_SUCCESS"
	hadoop fs -ls $HDFS_BASE_DIR/simi
    ;;

"corrMatrix")  
	echo  "running MR to transform item correlation to matrix form"
	CLASS_NAME=org.sifarish.common.CorrelationMatrixBuilder
	IN_PATH=$HDFS_BASE_DIR/simi
	OUT_PATH=$HDFS_BASE_DIR/matr
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -ls $HDFS_BASE_DIR/matr
    ;;

"impCorrMatrix")  
	echo  "importing correlation matrix to loal FS"
	hadoop fs -get $HDFS_BASE_DIR/matr/part-r-00000 $2
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
