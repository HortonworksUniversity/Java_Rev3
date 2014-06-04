#!/bin/bash

if [[ -z $1 ]];
then
  echo "Usage: $0 <class_version> [force|skip-images]"
  exit 1;
fi

SKIP_IMAGES=
FORCE=
if [[ $2 == "force" ]]; then
  FORCE=true
  echo "NOTE: Rebuilding classroom environment, removing any local changes..."
elif [[ $2 == "skip-images" ]]; then
  SKIP_IMAGES=true
  echo "NOTE: Skipping Docker image file updates..."
fi

REPO_DIR=$1
#Determine the course directory, which is the first part of the REPO_DIR up until the underscore character
#@arr = split(/_/, $REPO_DIR);
#arr=$(echo $REPO_DIR | tr "_" "\n")
COURSE_DIR=${REPO_DIR%_}
#COURSE_DIR=$arr[0]
mkdir /root/$COURSE_DIR
echo -e "Course files being copied to /root/$COURSE_DIR"

cd /root/$REPO_DIR
if [[ ! -z $FORCE ]];
then
  git reset HEAD --hard
fi
git pull

cd /root/dockerfiles
if [[ ! -z $FORCE ]];
then
  git reset HEAD --hard
fi
git pull

if [[ (-z $SKIP_IMAGES) || (! -z $FORCE) ]];
then

# Build the Docker images

# Build hwx/hdp_node_base first
cd /root/dockerfiles/hdp_node_base
x=$(docker images | grep -c  hwx/hdp_node_base)
if [ $x == 0 ]; then
        echo -e "\n*** Building hwx/hdp_node_base image... ***\n"
        docker build -t hwx/hdp_node_base .
        echo -e "Build of hwx/hdp_node_base complete!"
else
        echo -e "\n*** hwx/hdp_node_base image already built ***\n"
fi

# Build hwx/hdp_node
echo -e "\n*** Building hwx/hdp_node ***\n"
cd /root/dockerfiles/hdp_node
docker build -t hwx/hdp_node .
echo -e "\n*** Build of hwx/hdp_node complete! ***\n"

#If this script is execute multiple times, untagged images get left behind
#This command removes any untagged Docker images
docker rmi -f $(docker images | grep '^<none>' | awk '{print $3}')

fi


# Copy utility scripts into /root/scripts, which is already in the PATH
echo "Copying utility scripts..."
cp /root/dockerfiles/start_scripts/* /root/scripts/
cp /root/$REPO_DIR/scripts/* /root/scripts/

cp /root/dockerfiles/hdp_node/configuration_files/core_hadoop/* /etc/hadoop/conf/

#Copy lab files
mkdir /root/$COURSE_DIR/labs
cp -r /root/$REPO_DIR/labs/*  /root/$COURSE_DIR/labs/

#Replace /etc/hosts with one that contains the Docker server names
cp /root/scripts/hosts /etc/


echo -e "\n*** The lab environment has successfully been built for this classroom VM ***\n"