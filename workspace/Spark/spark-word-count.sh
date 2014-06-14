#!/bin/bash

cd /usr/lib/spark

SPARK_JAR=lib/spark-assembly_2.10-0.9.1.2.1.1.0-22-hadoop2.4.0.2.1.1.0-385.jar \
SPARK_YARN_APP_JAR=/root/spark-app.jar \
./bin/spark-class org.apache.spark.deploy.yarn.Client --jar /root/spark-app.jar --class spark.WordCount --args yarn-standalone --num-workers 2 --master-memory 512m --worker-memory 512m --worker-cores 1
