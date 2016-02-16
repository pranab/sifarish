#!/bin/bash

echo "running MR to generate implicit rating from event data"
JAR_NAME=/home/pranab/Projects/sifarish/target/sifarish-1.0.jar
CLASS_NAME=org.sifarish.feature.DiffTypeSimilarity
IN_PATH=/user/pranab/kiva/input
OUT_PATH=/user/pranab/kiva/output
echo "input $IN_PATH output $OUT_PATH"
hadoop fs -rmr $OUT_PATH
echo "removed output dir $OUT_PATH"
hadoop jar $JAR_NAME  $CLASS_NAME -Dconf.path=/home/pranab/Projects/bin/sifarish/kiva.properties  $IN_PATH  $OUT_PATH
hadoop fs -rmr $OUT_PATH/_logs
hadoop fs -rmr $OUT_PATH/_SUCCESS
hadoop fs -ls $OUT_PATH

