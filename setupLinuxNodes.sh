#!/bin/bash
rm -f config.json
cp linuxnode.json config.json
bash setuplin10.sh &
sleep 5
bash setuplin11.sh &
sleep 2
bash setuplin12.sh &
sleep 2
bash setuplin13.sh &
sleep 2
bash setuplin14.sh &
sleep 2
bash setuplin15.sh &
sleep 2
bash setuplin16.sh &
sleep 2
bash setuplin17.sh &
sleep 2
