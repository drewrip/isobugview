#!/bin/bash


touch jobs/${1}-running.txt

let rand_dur=$((5+ $RANDOM % 60))
echo "sleeping for $((rand_dur))"
sleep $rand_dur

cp sample_iso.log jobs/${1}-running.txt

mv jobs/${1}-running.txt jobs/${1}.txt
