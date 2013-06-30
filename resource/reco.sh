#!/bin/bash

base="$1"
echo "copy rating data"
hadoop fs -put  ratings.txt "${base}"/reco/item

echo "running rating stat MR"
./rast.sh

echo "running rating matrix MR"
./itsi.sh
hadoop fs -rmr "${base}"/reco/simi/_logs
hadoop fs -rmr "${base}"/reco/simi/_SUCCESS

echo "copying rating data to rating predictor input dir"
hadoop fs -put ratings.txt "${base}"/reco/simi

echo "copying rating stat file to predictor input dir"
hadoop fs -cp "${base}"/reco/stat/part-r-00000 /user/pranab/reco/simi/stat_ratings.txt


echo "running rating predictor MR"
./utpr.sh
hadoop fs -rmr "${base}"/reco/utpr/_logs
hadoop fs -rmr "${base}"/reco/utpr/_SUCCESS

echo "running rating aggregate predictor MR"
./utag.sh
hadoop fs -rmr "${base}"/reco/utag/_logs
hadoop fs -rmr "${base}"/reco/utag/_SUCCESS


