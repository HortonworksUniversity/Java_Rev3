#!/bin/bash

if [ "$NODE_TYPE" == "namenode" ] ; then
        #Start zookeeper
        su - zookeeper -c "echo 1 > /hadoop/zookeeper/data/myid"
        su - zookeeper -c 'source /etc/zookeeper/conf/zookeeper-env.sh ; export ZOOCFGDIR=/etc/zookeeper/conf;/usr/lib/zookeeper/bin/zkServer.sh start >> /var/log/zookeeper/zoo.out 2>&1'
	cat /root/conf/supervisor/nimbus-supervisord.conf >> /etc/supervisord.conf
elif [ "$NODE_TYPE" == "resourcemanager" ] ; then
	# Start zookeeper
        su - zookeeper -c "echo 2 > /hadoop/zookeeper/data/myid"
        su - zookeeper -c 'source /etc/zookeeper/conf/zookeeper-env.sh ; export ZOOCFGDIR=/etc/zookeeper/conf;/usr/lib/zookeeper/bin/zkServer.sh start >> /var/log/zookeeper/zoo.out 2>&1'
elif [[ ("$NODE_TYPE" == "hiveserver") ]]; then
	# Start zookeeper
        su - zookeeper -c "echo 3 > /hadoop/zookeeper/data/myid"
        su - zookeeper -c 'source /etc/zookeeper/conf/zookeeper-env.sh ; export ZOOCFGDIR=/etc/zookeeper/conf;/usr/lib/zookeeper/bin/zkServer.sh start >> /var/log/zookeeper/zoo.out 2>&1'
elif [[ ("$NODE_TYPE" == "workernode") ]]; then
   host=`hostname`
   ln -s /opt/kafka/config/server-${host}.properties /opt/kafka/config/server.properties
   cat /root/conf/supervisor/worker-supervisord.conf >> /etc/supervisord.conf
   #sed -i "/.*logviewer.port:.*/c\logviewer.port: $LOG_PORT" /etc/storm/conf/storm.yaml
fi

chkconfig supervisord on
/etc/init.d/supervisord start

