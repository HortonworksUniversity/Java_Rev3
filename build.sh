#!/bin/bash

. /root/dockerfiles/start_scripts/build.sh $@ && (echo "Parent build.sh failed"; exit 1)

# Build hwx/hdp_storm_node
echo -e "\n*** Building hwx/hdp_storm_node ***\n"
cd /root/$REPO_DIR/dockerfiles/hdp_storm_node
docker build -t hwx/hdp_storm_node .
echo -e "\n*** Build of hwx/hdp_storm_node complete! ***\n"

remove_untagged_images.sh

echo -e "\n*** The lab environment has successfully been built for this classroom VM ***\n"
