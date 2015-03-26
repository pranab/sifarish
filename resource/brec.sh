#!/bin/bash
# contains everything needed to execute sifarish in batch mode

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi
	
JAR_NAME=/home/pranab/Projects/sifarish/target/sifarish-1.0.jar
CHOMBO_JAR_NAME=/home/pranab/Projects/chombo/target/chombo-1.0.jar
HDFS_BASE_DIR=/user/pranab/reco
PROP_FILE=/home/pranab/Projects/bin/sifarish/reco.properties
HDFS_META_BASE_DIR=/user/pranab/meta/imra

case "$1" in

"genExplicitRating")  
	echo "generating explicit rating data for recommendation based on explicit rating only"
	ruby ratings.rb $2 $3 $4  > $5
    ;;

"expExplicitRating")  
	echo "exporting rating data for recommendation based on explicit rating only to HDFS"
	hadoop fs -rmr $HDFS_BASE_DIR/rate/*
	hadoop fs -put $2 $HDFS_BASE_DIR/rate
	hadoop fs -ls $HDFS_BASE_DIR/rate
    ;;

"createExplicitRating")  
	# usage ./brec.sh createExplicitRating <implicit_rating_file> <percentage_rated> <output_file>
	echo "generating post conversion explicit rating data based implicit rating data"
	./explicit_rating.py $2 $3 > $4
    ;;

"putExplicitRating")  
	echo "exporting various explicit rating data to HDFS"
	if [ "$3" = "clean" ]
	then
		hadoop fs -rmr $HDFS_BASE_DIR/erat/*
	fi
	
	hadoop fs -put $2 $HDFS_BASE_DIR/erat
	hadoop fs -ls $HDFS_BASE_DIR/erat
    ;;


"expSchema")  
	echo "exporting event mapping metadata to HDFS"
	hadoop fs -rmr $HDFS_META_BASE_DIR/*
	hadoop fs -put $2 $HDFS_META_BASE_DIR
	hadoop fs -ls $HDFS_META_BASE_DIR
    ;;
 
"genHistEvent")  
	# usage ./brec.sh <item_count> <user_count> <average_event_count_per_user>
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
	IN_PATH=$HDFS_BASE_DIR/$2
	OUT_PATH=$HDFS_BASE_DIR/crat
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/crat/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/crat/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/crat
    ;;

"blendRating")  
	echo "running MR to ratings from different signals"
	CLASS_NAME=org.sifarish.common.RatingBlender
	IN_PATH=$HDFS_BASE_DIR/rate,$HDFS_BASE_DIR/erat
	OUT_PATH=$HDFS_BASE_DIR/brat
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/brat/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/brat/_SUCCESS
	hadoop fs -ls $HDFS_BASE_DIR/brat
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
	IN_PATH=$HDFS_BASE_DIR/crat
	OUT_PATH=$HDFS_BASE_DIR/stat
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/stat/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/stat/_SUCCESS	
	;;
	
"renameRatingStat")	
	echo "renaming rating stat file"
	hadoop fs -cp $HDFS_BASE_DIR/stat/part-r-00000 $HDFS_BASE_DIR/stat/$2
	hadoop fs -ls $HDFS_BASE_DIR/simi
	;;

"renameRatingFile")	
	echo "renaming rating  file"
	hadoop fs -mv $HDFS_BASE_DIR/crat/$2 $HDFS_BASE_DIR/crat/$3  
	hadoop fs -ls $HDFS_BASE_DIR/crat
	;;
	
"ratingPred")
	echo "running MR for rating predictor"
	CLASS_NAME=org.sifarish.common.UtilityPredictor
	if [ "$2" = "withStat" ]
	then
		IN_PATH=$HDFS_BASE_DIR/crat,$HDFS_BASE_DIR/stat,$HDFS_BASE_DIR/simi
	else
		IN_PATH=$HDFS_BASE_DIR/crat,$HDFS_BASE_DIR/simi
	fi
	OUT_PATH=$HDFS_BASE_DIR/utpr
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/utpr/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/utpr/_SUCCESS	
	hadoop fs -ls $HDFS_BASE_DIR/utpr
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
	hadoop fs -rmr $HDFS_BASE_DIR/utag/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/utag/_SUCCESS	
	hadoop fs -ls $HDFS_BASE_DIR/utag
	;;

"storeBizData")  
	echo "exporting biz data to HDFS bizg"
	hadoop fs -put $2 $HDFS_BASE_DIR/bizg/$3
	hadoop fs -ls $HDFS_BASE_DIR/bizg
    ;;

"injectBizGoal")	
	CLASS_NAME=org.sifarish.common.BusinessGoalInjector
	echo "running MR to inject business goal to recommendation score"
	IN_PATH=$HDFS_BASE_DIR/utag,$HDFS_BASE_DIR/bizg
	OUT_PATH=$HDFS_BASE_DIR/bigo
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/bigo/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/bigo/_SUCCESS	
	;;

"genEngageDistr")	
	CLASS_NAME=org.sifarish.common.ItemEngagementDistr
	echo "running MR to generate per user item engaement distribution"
	IN_PATH=$HDFS_BASE_DIR/rate
	OUT_PATH=$HDFS_BASE_DIR/endi
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/endi/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/endi/_SUCCESS	
	;;
	
"genItemNovelty")	
	CLASS_NAME=org.sifarish.common.IndividualNovelty
	echo "running MR to generate per user item novelty from implicit rating"
	IN_PATH=$HDFS_BASE_DIR/endi
	OUT_PATH=$HDFS_BASE_DIR/novl
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/novl/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/novl/_SUCCESS	
	;;

"renamePredRatingFile")	
	hadoop fs -mv $HDFS_BASE_DIR/utag/$2 $HDFS_BASE_DIR/utag/$3  
	hadoop fs -ls $HDFS_BASE_DIR/utag
	;;

"joinRatingNovelty")	
	CLASS_NAME=org.chombo.mr.Joiner
	echo "running MR to join predicted rating with novelty"
	IN_PATH=$HDFS_BASE_DIR/utag,$HDFS_BASE_DIR/novl
	OUT_PATH=$HDFS_BASE_DIR/rano
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $CHOMBO_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/rano/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/rano/_SUCCESS	
	;;

"injectItemNovelty")	
	CLASS_NAME=org.chombo.mr.WeightedAverage
	echo "running MR to generate per user item novelty from implicit rating"
	IN_PATH=$HDFS_BASE_DIR/rano
	OUT_PATH=$HDFS_BASE_DIR/utno
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $CHOMBO_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/utno/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/utno/_SUCCESS	
	;;

"sortByUser")	
	CLASS_NAME=org.chombo.mr.TextSorter
	echo "running TextSorter mr ro sort by userID"
	IN_PATH=/user/pranab/reco/$2
	OUT_PATH=/user/pranab/reco/usso
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $CHOMBO_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/usso/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/usso/_SUCCESS	
	;;


"itemPopularity")	
	CLASS_NAME=org.chombo.mr.WeightedAverage
	echo "running MR for global item popularity"
	IN_PATH=$HDFS_BASE_DIR/stat
	OUT_PATH=$HDFS_BASE_DIR/popu
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $CHOMBO_JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/popu/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/popu/_SUCCESS	
	;;

"renameRating")
	echo "renaming rating data file"
	hadoop fs -mv $HDFS_BASE_DIR/rate/$2 $HDFS_BASE_DIR/rate/$3  
	hadoop fs -ls $HDFS_BASE_DIR/rate
	;;

"posFeedbackReorder") 
	CLASS_NAME=org.sifarish.common.PositiveFeedbackBasedRankReorderer
	echo "running MR for positive feedback driven rank reordering"
	IN_PATH=$HDFS_BASE_DIR/utag,$HDFS_BASE_DIR/rate
	OUT_PATH=$HDFS_BASE_DIR/pfrr
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/pfrr/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/pfrr/_SUCCESS	
	hadoop fs -ls $HDFS_BASE_DIR/pfrr
    ;;

"genItemAttrData")  
	echo "generating item attribute data"
	echo "usage: ./item_cat_brand.py <event_file>"
	./item_cat_brand.py $2
    ;;

"storeItemAttrData")  
	echo "exporting item attribute data to HDFS dir itat"
	hadoop fs -put $2 $HDFS_BASE_DIR/itat/$3
	hadoop fs -ls $HDFS_BASE_DIR/itat
    ;;
    	
"userItemAttrAggr") 
	CLASS_NAME=org.sifarish.common.ItemRatingAttributeAggregator
	echo "running MR for Item predicted rating and attribute aggregation"
	IN_PATH=$HDFS_BASE_DIR/utag,$HDFS_BASE_DIR/itat
	OUT_PATH=$HDFS_BASE_DIR/iraa
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/iraa/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/iraa/_SUCCESS	
	hadoop fs -ls $HDFS_BASE_DIR/iraa
    ;;

"renameUserItemAttrData")  
	echo "renaming user item attribute data"
	hadoop fs -mv $HDFS_BASE_DIR/iraa/$2 $HDFS_BASE_DIR/iraa/$3
	hadoop fs -ls $HDFS_BASE_DIR/iraa
    ;;

"diversifyWithAttr") 
	CLASS_NAME=org.sifarish.common.AttributeBasedDiversifier
	echo "running MR for attribute based diversifier"
	IN_PATH=$HDFS_BASE_DIR/utag,$HDFS_BASE_DIR/iraa
	OUT_PATH=$HDFS_BASE_DIR/abdi
	echo "input $IN_PATH output $OUT_PATH"
	hadoop fs -rmr $OUT_PATH
	echo "removed output dir $OUT_PATH"
	hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=$PROP_FILE  $IN_PATH  $OUT_PATH
	hadoop fs -rmr $HDFS_BASE_DIR/abdi/_logs
	hadoop fs -rmr $HDFS_BASE_DIR/abdi/_SUCCESS	
	hadoop fs -ls $HDFS_BASE_DIR/abdi
    ;;

*) 
	echo "unknown operation $1"
	;;

esac
	
		
