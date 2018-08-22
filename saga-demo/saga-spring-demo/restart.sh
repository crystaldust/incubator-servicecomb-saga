#!/bin/bash
./saga-demo.sh down
nohup ./saga-demo.sh up > saga.log &
tail -f ./saga.log
