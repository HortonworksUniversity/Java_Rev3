#
#
#
FROM hwxu/hdp_kafka_node
MAINTAINER Rich Raposa, rich@hortonworks.com

# Copy over custom scripts and config files
ADD conf/ /root/conf/
ADD start-scripts/ /root/start-scripts/

# Install configs and enable supervisor daemon
RUN cp /root/conf/storm/* /etc/storm/conf

