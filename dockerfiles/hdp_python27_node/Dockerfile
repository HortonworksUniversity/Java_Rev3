#
#
#
FROM hwxu/hdp_node
MAINTAINER Rich Raposa, rich@hortonworks.com

# Install Python 2.7
RUN yum -y groupinstall "Development tools"
RUN yum -y install zlib-devel bzip2-devel openssl-devel ncurses-devel sqlite-devel readline-devel tk-devel gdbm-devel db4-devel libpcap-devel xz-devel
RUN echo "/usr/local/lib" >> /etc/ld.so.conf
RUN wget http://python.org/ftp/python/2.7.6/Python-2.7.6.tar.xz
RUN tar xf Python-2.7.6.tar.xz
RUN cd Python-2.7.6 && ./configure --prefix=/usr/local --enable-unicode=ucs4 --enable-shared LDFLAGS="-Wl,-rpath /usr/local/lib" && make && make altinstall
RUN rm Python-2.7.6.tar.xz

RUN wget https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py
RUN python2.7 ez_setup.py
RUN easy_install-2.7 pip
RUN rm -f ez_setup.py
