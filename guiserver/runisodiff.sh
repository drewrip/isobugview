#!/bin/bash


touch jobs/${1}/running.txt
mkdir jobs/${1}/map
mkdir jobs/${1}/sql
cp -r ./sql_process/* jobs/${1}/.
cd jobs/${1}
mv pglast_sqlparser_0916.py sqlparser.py
mv log_parser.py logparser.py

ls -R

echo "Starting Analysis"

# Parse SQL log against schema
./sqlparser.py sql.log schema.csv ./

# Generate configs
./logparser.py pglast_schema.csv.txt

echo "Finished Parsing"

../../checker -f conf/pglast.conf -p 40 -k 5 -n 5 -u ex -i rc -r 123456 -m n -j 15 -s b > running.txt

mv running.txt report.log
