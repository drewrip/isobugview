#!/bin/bash

touch jobs/${1}/running.json
cd jobs/${1}

echo "Starting Recheck"

echo "./checker -f split/pglast_app.json -p ${2} -k ${3} -n ${4} -u ex -i ${5} -r ${6} -m n -j ${7} -g row -s ${8} -c ${9} -o running.json >> server_logs/checker.log 2>&1"

./checker -f split/pglast_app.json -p ${2} -k ${3} -n ${4} -u ex -i ${5} -r ${6} -m n -j ${7} -g row -s ${8} -c ${9} -o running.json >> server_logs/checker.log 2>&1

mv running.json finished.json
