#!/bin/bash
# contains everything needed to execute sifarish in batch mode

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi
	
JAR_NAME=/home/pranab/Projects/sifarish/target/sifarish-1.0.jar
HDFS_BASE_DIR=/user/pranab/reco
PROP_FILE=/home/pranab/Projects/bin/sifarish/reco.properties

case "$1" in
"expSchema")  
	echo "exporting event mapping metadata to HDFS"
	hadoop fs -rmr /user/pranab/meta/imra/*
	hadoop fs -put engageEvent.json /user/pranab/meta/imra
	hadoop fs -ls /user/pranab/meta/imra
    ;;
 
"genHistEvent")  
	echo "generating historical event data"
	./engage.rb $2 $3 $4 > $5
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
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/rate/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/rate/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/rate
    ;;

"compactRating")  
	echo "running MR to format rating to compact form"
	CLASS_NAME=org.sifarish.common.CompactRatingFormatter
	IN_PATH=$HDFS_BASE_DIR/rate
	OUT_PATH=$HDFS_BASE_DIR/crat
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/crat/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/crat/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/crat
    ;;

"correlation")  
	echo  "running MR to generate item correlation from rating data"
	CLASS_NAME=org.sifarish.common.ItemDynamicAttributeSimilarity
	IN_PATH=$HDFS_BASE_DIR/crat
	OUT_PATH=$HDFS_BASE_DIR/simi
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/simi/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/simi/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/simi
    ;;

"ratingStat")
	echo "running MR to generate stating stats"
	CLASS_NAME=org.sifarish.social.ItemRatingStat
	IN_PATH=$HDFS_BASE_DIR/rate
	OUT_PATH=$HDFS_BASE_DIR/stat
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	;;
	
"copyRatingStat")	
	echo "copying rating stat file to rating predictor input dir"
	hadoop fs -cp $HDFS_BASE_DIR/stat/part-r-00000 $HDFS_BASE_DIR/simi/stat_ratings.txt
	;;

"ratingPred")
	echo "running MR for rating predictor"
	CLASS_NAME=org.sifarish.common.UtilityPredictor
	IN_PATH=$HDFS_BASE_DIR/simi
	OUT_PATH=$HDFS_BASE_DIR/utpr
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/reco/utpr/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/reco/utpr/_SUCCESS	
	;;

"ratingAggr")	
	CLASS_NAME=org.sifarish.common.UtilityAggregator
	echo "running MR to aggregate predicted rating"
	IN_PATH=$HDFS_BASE_DIR/utpr
	OUT_PATH=$HDFS_BASE_DIR/utag
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/reco/utag/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/reco/utag/_SUCCESS	
	;;
	
*) 
	echo "unknown operation $1"
	;;

esac
	
		
