#!/bin/bash

if [ "$NODE_TYPE" == "namenode" ] ; then
  cat /root/conf/supervisor/nimbus-supervisord.conf >> /etc/supervisord.conf
elif [[ ("$NODE_TYPE" == "workernode") ]]; then
  cat /root/conf/supervisor/stormworker-supervisord.conf >> /etc/supervisord.conf
fi
