#!/bin/bash

touch jobs/${1}/running.json
mkdir jobs/${1}/map
mkdir jobs/${1}/sql
mkdir jobs/${1}/conf
mkdir jobs/${1}/conf/txn
mkdir jobs/${1}/conf/schema
mkdir jobs/${1}/server_logs

cp ../db-isolation/checker jobs/${1}/.
cd jobs/${1}

ln -s ../../../db-isolation/sql_process/pglast_sqlparser.py .
ln -s ../../../db-isolation/sql_process/log_parser.py .

echo "Starting Analysis"

# temporary fix for app name problem

mv app.log pglast_tpcc.log
mv app_db_info.csv tpcc_db_info.csv

# Parse SQL log against schema
python3 pglast_sqlparser.py pglast_tpcc.log tpcc_db_info.csv . >> server_logs/sql_parser.log 2>&1

# part 2 of temporary fix for app name problem

mv pglast_tpcc.log app.log
mv tpcc_db_info.csv app_db_info.csv
mv pglast_tpcc.txt pglast_app.txt

# Generate configs
python3 log_parser.py -l pglast_app.txt -x app_db_info.csv >> server_logs/log_parser.log 2>&1

mv app_db_info.csv conf/schema/app_db_info.csv

#echo "schema_file = conf/schema/app_db_info.csv" >> conf/pglast_app.conf

echo "Finished Parsing"

./checker -f conf/pglast_app.json -p ${2} -k ${3} -n ${4} -u ex -i ${5} -r ${6} -m n -j ${7} -g row -s ${8} -c ${9} -o running.json >> server_logs/checker.log 2>&1

mv running.json finished.json
