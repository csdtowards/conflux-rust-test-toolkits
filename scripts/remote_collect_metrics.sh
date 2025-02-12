#!/bin/bash

tar cvfz metrics.log.tgz 'file=/tmp/`ls -t /tmp | grep conflux_test_ | head -n 1`/node0/'metrics.log'