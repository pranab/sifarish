#!/bin/bash

if [ $# -lt 1 ]
then
        echo "Usage : $0 operation"
        exit
fi
	
JAR_NAME=/home/pranab/Projects/sifarish/target/sifarish-1.0.jar
STORM_HOME=/home/pranab/Tools/storm-0.8.2
HDFS_BASE_DIR=/user/pranab/trend
PROP_FILE=/home/pranab/Projects/bin/sifarish/trending.properties

case "$1" in
"genEvents")  
	# ./trending.py <num_users>  <num_items> <num_sessions>
	echo  "generating events and pushing to redis queue"
	./trending.py genEvents $2 $3 $4
    ;;

"buildJar")
	echo  "building uber jar"
	ant -f build_storm.xml
	;;

"startStorm")
	echo  "starting storm"
	$STORM_HOME/bin/storm nimbus &
	sleep 5
	$STORM_HOME/bin/storm supervisor & 
	sleep 5
	$STORM_HOME/bin/storm ui &
	;;
	
"deployTopology")
	echo  "deploying sifarish storm topology"
	$STORM_HOME/bin/storm  jar uber-sifarish-1.0.jar  org.sifarish.realtime.TrendingTopology  trending trending.properties
	;;
	
"killTopology")
	echo  "killing sifarish storm topology"
	$STORM_HOME/bin/storm  kill  trending
	;;

"showTopHittersQueue")  
	echo  "reading top hitters from redis queue"
	./trending.py showTopHittersQueue
    ;;

*) 
	echo "unknown operation $1"
	;;

esac
	
