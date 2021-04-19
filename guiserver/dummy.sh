#!/bin/bash


touch jobs/${1}-running.txt

let rand_dur=$((5+ $RANDOM % 60))
echo "sleeping for $((rand_dur))"
sleep $rand_dur


echo "RESULTS FROM ISODIFF FOR ${1}" >> jobs/${1}.txt

mv jobs/${1}-running.txt jobs/${1}.txt
