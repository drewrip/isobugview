#!/bin/bash


touch jobs/${1}/running.txt

echo "Starting Analysis"

# Parse SQL log against schema
./sql_process/pglast_sqlparser_0916.py jobs/${1}/sql.log jobs/${1}/schema.csv jobs/${1}/

ls
ls sql_process/map/
ls -R jobs/
# Generate configs
./sql_process/log_parser.py jobs/${1}/pglast_schema.csv.txt

echo "Finished Parsing"

./checker -f jobs/${1}/conf/pglast.conf -p 40 -k 5 -n 5 -u ex -i rc -r 123456 -m n -j 15 -s b > jobs/${1}/running.txt

mv jobs/${1}/running.txt jobs/${1}/report.log
