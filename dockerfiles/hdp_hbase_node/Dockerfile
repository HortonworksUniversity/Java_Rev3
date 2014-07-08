#
#
#
FROM hwxu/hdp_zookeeper_node
MAINTAINER Rich Raposa, rich@hortonworks.com

# Copy over custom scripts and config files
ADD start-scripts/ /root/start-scripts/
ADD conf/ /root/conf

RUN cp /root/conf/hbase/* /etc/hbase/conf

