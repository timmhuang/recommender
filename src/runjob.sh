#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR/builds/

hdfs dfs -rm -r /dataDividedByUser
hdfs dfs -rm -r /coOccurrenceMatrix
hdfs dfs -rm -r /Normalize
hdfs dfs -rm -r /Multiplication
hdfs dfs -rm -r /Sum

hadoop jar recommender.jar Driver /input /dataDividedByUser /coOccurrenceMatrix /Normalize /Multiplication /Sum