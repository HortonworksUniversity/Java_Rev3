#!/bin/bash

. /root/dockerfiles/start_scripts/build.sh $@ && (echo "Parent build.sh failed"; exit 1)

apt-get install curl
cp /root/dockerfiles/hdp_node/configuration_files/hive/* /etc/hive/conf/

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


echo -e "\n*** The lab environment has successfully been built for this classroom VM ***\n"
