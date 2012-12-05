#!/bin/bash

mkdir /tmp/cephtest
wget https://raw.github.com/ceph/ceph/master/qa/run_xfstests.sh
chmod +x run_xfstests.sh
./run_xfstests.sh -c 1 -f xfs -t /dev/vdb -s /dev/vdc 1-10
