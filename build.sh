#!/bin/bash

. /root/dockerfiles/start_scripts/build.sh $@ && (echo "Parent build.sh failed"; exit 1)

apt-get install curl

# Build hwxu/hdp_zookeeper_node
echo -e "\n*** Building hwux/hdp_zookeeper_node ***\n"
cd /root/dockerfiles/hdp_zookeeper_node
docker build -t hwxu/hdp_zookeeper_node .
echo -e "\n*** Build of hwxu/hdp_zookeeper_node complete! ***\n"

# Build hwxu/hdp_hbase_node
echo -e "\n*** Building hwux/hdp_hbase_node ***\n"
cd /root/dockerfiles/hdp_hbase_node
docker build -t hwxu/hdp_hbase_node .
echo -e "\n*** Build of hwxu/hdp_hbase_node complete! ***\n"

# Build hwxu/hdp_kafka_node
echo -e "\n*** Building hwux/hdp_kafka_node ***\n"
cd /root/$REPO_DIR/dockerfiles/hdp_kafka_node
docker build -t hwxu/hdp_kafka_node .
echo -e "\n*** Build of hwxu/hdp_kafka_node complete! ***\n"

# Build hwxu/hdp_storm_node
echo -e "\n*** Building hwux/hdp_storm_node ***\n"
cd /root/$REPO_DIR/dockerfiles/hdp_storm_node
docker build -t hwxu/hdp_storm_node .
echo -e "\n*** Build of hwxu/hdp_storm_node complete! ***\n"

# Build hwxu/hdp_spark_node
#echo -e "\n*** Building hwux/hdp_spark_node ***\n"
#cd /root/$REPO_DIR/dockerfiles/hdp_spark_node
#docker build -t hwxu/hdp_spark_node .
#echo -e "\n*** Build of hwxu/hdp_spark_node complete! ***\n"

# Build hwxu/hdp_spark_storm_node
#echo -e "\n*** Building hwux/hdp_spark_storm_node ***\n"
#cd /root/$REPO_DIR/dockerfiles/hdp_spark_storm_node
#docker build -t hwxu/hdp_spark_storm_node .
#echo -e "\n*** Build of hwxu/hdp_spark_storm_node complete! ***\n"

remove_untagged_images.sh

# Copy Eclipse workspace files
mkdir -p /root/$COURSE_DIR/workspace
cp -ar /root/$REPO_DIR/workspace  /root/$COURSE_DIR/

echo -e "\n*** The lab environment has successfully been built for this classroom VM ***\n"
