#!/bin/bash

if [[ ("$NODE_TYPE" == "workernode") ]]; then
   host=`hostname`
   ln -s /opt/kafka/config/server-${host}.properties /opt/kafka/config/server.properties
   cat /root/conf/supervisor/kafka-supervisord.conf >> /etc/supervisord.conf
fi
