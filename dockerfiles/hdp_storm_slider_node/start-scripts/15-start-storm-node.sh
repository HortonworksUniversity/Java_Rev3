#!/bin/bash

if [[ ("$NODE_TYPE" == "workernode") ]]; then
   host=`hostname`
   if [[ $host == "node4" ]]; then
     #wait for nodes to come up before finishing Slider/Storm deployment
     sleep 30
     su -l hdfs -c "hadoop fs -mkdir -p /slider/agent/conf"
     su -l hdfs -c "hadoop fs -chown -R root:root /slider"
     hadoop fs -put /opt/slider/agent/slider-agent.tar.gz /slider/agent
     hadoop fs -put /opt/slider/agent/conf/agent.ini /slider/agent/conf
     hadoop fs -put /root/conf/slider/storm/storm_v091.zip /slider
     /opt/slider/bin/slider create storm --image hdfs://namenode:8020/slider/agent/slider-agent.tar.gz --template /root/conf/slider/storm/appConfig.json --resources /root/conf/slider/storm/resources.json
   fi
fi

