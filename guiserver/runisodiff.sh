#!/bin/bash

touch jobs/${1}/running.json
mkdir jobs/${1}/map
mkdir jobs/${1}/sql
mkdir jobs/${1}/conf
mkdir jobs/${1}/conf/txn
mkdir jobs/${1}/conf/schema

cp ../db-isolation/checker jobs/${1}/.
cd jobs/${1}

ln ../../../db-isolation/sql_process/pglast_sqlparser.py .
ln ../../../db-isolation/sql_process/log_parser.py .

echo "Starting Analysis"
# Parse SQL log against schema
python3 pglast_sqlparser.py app.log app_db_info.csv .

# Generate configs
python3 log_parser.py pglast_app.txt app_db_info.csv

mv app_db_info.csv conf/schema/app_db_info.csv

#echo "schema_file = conf/schema/app_db_info.csv" >> conf/pglast_app.conf

ls conf/
cat conf/pglast_app.conf

echo "Finished Parsing"

./checker -f conf/pglast_app.conf -p 8 -k 3 -n 3 -u ex -i rc -r 123456 -m n -j 15 -g row -s b -c 15 -o running.json

mv running.json finished.json
