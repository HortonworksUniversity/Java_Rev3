#
#
#
FROM hwxu/hdp_zookeeper_node
MAINTAINER Rich Raposa, rich@hortonworks.com

# Install Kafka

RUN wget http://public-repo-1.hortonworks.com/HDP-LABS/Projects/kafka/0.8.1/centos6/kafka-0.8.1.2.1.4.0-632.el6.noarch.rpm
RUN rpm -ivh kafka-0.8.1.2.1.4.0-632.el6.noarch.rpm
RUN rm -f kafka-0.8.1.2.1.4.0-632.el6.noarch.rpm
RUN rm -f /etc/kafka/conf/server.properties
RUN echo "export PATH=/usr/lib/kafka/bin:$PATH" >> /etc/profile

# Copy over custom scripts and config files
ADD conf/ /root/conf/
ADD start-scripts/ /root/start-scripts/

# Install Kafka config files
RUN cp /root/conf/kafka/* /etc/kafka/conf
