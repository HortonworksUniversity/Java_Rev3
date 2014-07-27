#
#
#
FROM hwxu/hdp_zookeeper_node
MAINTAINER Rich Raposa, rich@hortonworks.com

# Install Kafka
RUN wget http://mirrors.ibiblio.org/apache/kafka/0.8.1.1/kafka_2.10-0.8.1.1.tgz -O /opt/kafka.tgz
RUN tar -C /opt -zxvf /opt/kafka.tgz
RUN rm -f /opt/kafka.tgz
RUN ln -s /opt/kafka_2.10-0.8.1.1/ /opt/kafka 
RUN rm -f /opt/kafka/config/server.properties
RUN echo "export PATH=/opt/kafka/bin:$PATH" >> /etc/profile

# Copy over custom scripts and config files
ADD conf/ /root/conf/
ADD start-scripts/ /root/start-scripts/

# Install Kafka config files
RUN cp /root/conf/kafka/* /opt/kafka/config
