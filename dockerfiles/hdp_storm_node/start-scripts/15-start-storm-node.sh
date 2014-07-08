#!/bin/bash

if [ "$NODE_TYPE" == "namenode" ] ; then
	cat /root/conf/supervisor/nimbus-supervisord.conf >> /etc/supervisord.conf
elif [[ ("$NODE_TYPE" == "workernode") ]]; then
   cat /root/conf/supervisor/worker-supervisord.conf >> /etc/supervisord.conf
fi

/etc/init.d/supervisord restart
