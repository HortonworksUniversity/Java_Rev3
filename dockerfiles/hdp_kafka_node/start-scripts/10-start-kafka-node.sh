#!/bin/bash

if [[ ("$NODE_TYPE" == "workernode") ]]; then
   host=`hostname`
   ln -s /etc/kafka/conf/server-${host}.properties /etc/kafka/conf/server.properties
   cat /root/conf/supervisor/kafka-supervisord.conf >> /etc/supervisord.conf
fi
