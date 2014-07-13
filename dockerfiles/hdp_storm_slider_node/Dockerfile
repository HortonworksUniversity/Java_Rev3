#
#
#
FROM hwxu/hdp_kafka_node
MAINTAINER Rich Raposa, rich@hortonworks.com

# Install Slider 
RUN wget http://public-repo-1.hortonworks.com/slider/0.30/slider-0.30-all.tar -O /opt/slider.tar
RUN tar -C /opt -xvf /opt/slider.tar
RUN ln -s /opt/slider-0.30 /opt/slider
RUN rm -f /opt/slider.tar
RUN echo "export PATH=$PATH:/opt/slider/bin" >> /etc/profile

ADD conf/ /root/conf/
ADD start-scripts/ /root/start-scripts/

# Install custom yarn-site.xml
RUN cp /root/conf/hadoop/yarn-site.xml /etc/hadoop/conf

# Install slider client config file
RUN cp /root/conf/slider/slider-client.xml /opt/slider/conf/slider-client.xml
RUN cp /root/conf/slider/agent.ini /opt/slider/agent/conf/agent.ini

