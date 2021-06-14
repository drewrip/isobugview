#!/bin/bash


touch jobs/${1}/running.txt
mkdir jobs/${1}/map
mkdir jobs/${1}/sql
mkdir jobs/${1}/conf
mkdir jobs/${1}/conf/txn
mkdir jobs/${1}/conf/schema

cp ./sql_process/pglast_sqlparser_0916.py jobs/${1}/pglast_sqlparser_0916.py
cp ./sql_process/log_parser.py jobs/${1}/log_parser.py
cp checker jobs/${1}/.
cd jobs/${1}

ls -R

echo "Starting Analysis"
# Parse SQL log against schema
python3 pglast_sqlparser_0916.py sql.log schema.csv ./

# Generate configs
python3 log_parser.py pglast_schema.csv.txt

mv schema.csv conf/schema/schema.csv

echo "schema_file = conf/schema/schema.csv" >> conf/pglast_schema.conf

echo "Finished Parsing"


./checker -f conf/pglast_schema.conf -p 1 -k 2 -n 2 -u ex -i rc -r 123456 -m n -j 15 -s b > running.txt

cat running.txt

mv running.txt report.log
