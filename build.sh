#!/bin/bash

. /root/dockerfiles/start_scripts/build.sh $@ && (echo "Parent build.sh failed"; exit 1)

# Build hwx/hdp_storm_node
echo -e "\n*** Building hwx/hdp_storm_node ***\n"
cd /root/$REPO_DIR/dockerfiles/hdp_storm_node
docker build -t hwx/hdp_storm_node .
echo -e "\n*** Build of hwx/hdp_storm_node complete! ***\n"

remove_untagged_images.sh

#Install Gradle and download dependencies
if [[ ! -d /root/.gvm ]];
then
  curl -s get.gvmtool.net | bash
  source "/root/.gvm/bin/gvm-init.sh"
  echo "gvm_auto_answer=true" > $GVM_DIR/etc/config
  echo "gvm_auto_selfupdate=false" >> $GVM_DIR/etc/config
  gvm install gradle
  cd /root/$COURSE_DIR/labs
  gradle dependencies
fi


echo -e "\n*** The lab environment has successfully been built for this classroom VM ***\n"
