#
#
#
FROM hwxu/hdp_node
MAINTAINER Rich Raposa, rich@hortonworks.com

# Install supervisor daemon
RUN wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
RUN wget http://rpms.famillecollet.com/enterprise/remi-release-6.rpm
RUN rpm -Uvh remi-release-6*.rpm epel-release-6*.rpm
RUN rm -f remi-release-6*.rpm epel-release-6*.rpm
RUN yum -y install supervisor

# Copy over custom scripts and config files
ADD storm-conf/ /root/storm-conf/
ADD supervisor-conf /root/supervisor-conf
ADD start-scripts/ /root/start-scripts/

# Install configs and enable supervisor daemon
RUN cp /root/storm-conf/* /etc/storm/conf

