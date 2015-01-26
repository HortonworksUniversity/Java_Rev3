#!/bin/bash

. /root/dockerfiles/start_scripts/build.sh $@ && (echo "Parent build.sh failed"; exit 1)

# The repos have changed, so this code updates the repos on the local Ubuntu host
rm -rf /etc/apt/sources.list.d/hdp.list
wget http://public-repo-1.hortonworks.com/HDP/ubuntu12/2.1.5.0/hdp.list -O /etc/apt/sources.list.d/hdp.list

apt-get update
apt-get -y install curl

#Oozie-client is needed until the VM is updated
apt-get -y install oozie-client

# Build hwxu/hdp_zookeeper_node
echo -e "\n*** Building hwux/hdp_node ***\n"
cd /root/dockerfiles/hdp_node
docker build -t hwxu/hdp_node .
echo -e "\n*** Build of hwxu/hdp_node complete! ***\n"



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
#echo -e "\n*** Building hwux/hdp_kafka_node ***\n"
#cd /root/$REPO_DIR/dockerfiles/hdp_kafka_node
#docker build -t hwxu/hdp_kafka_node .
#echo -e "\n*** Build of hwxu/hdp_kafka_node complete! ***\n"

# Build hwxu/hdp_storm_node
#echo -e "\n*** Building hwux/hdp_storm_node ***\n"
#cd /root/$REPO_DIR/dockerfiles/hdp_storm_node
#docker build -t hwxu/hdp_storm_node .
#echo -e "\n*** Build of hwxu/hdp_storm_node complete! ***\n"

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

#Unzip eclipse
if [[ ! -d "/root/eclipse" ]]; then
	cd /root/
	tar -xvf /root/eclipse.tgz
	chown -R root:root /root/eclipse
fi

# Copy Eclipse workspace files
mkdir -p /root/$COURSE_DIR/workspace
cp -ar /root/$REPO_DIR/workspace  /root/$COURSE_DIR/


#Fix an annoying bug w/ Eclipse
mkdir -p /root/java/workspace/.metadata/.plugins/org.eclipse.core.resources/.projects/RemoteSystemsTempFiles/

#Remove /root/workspace to avoid any confusion
rm -rf /root/workspace

#Fix bug in /etc/hosts (JIRA TRNG-715)
echo "127.0.0.1       localhost     ubuntu" >> /etc/hosts

echo -e "\n*** The lab environment has successfully been built for this classroom VM ***\n"
