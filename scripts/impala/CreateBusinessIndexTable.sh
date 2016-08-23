#!/bin/bash
##
#
# Impala script to create a table of Business Index records from Deduplication output
#
# Should be passed a -d argument for the database name to create the business index table in
# Should be passed a -i argument for the Impala deamon to connect to
# Should be passed a -b argument for the business index path in HDFS
#
##

OPTIND=1

displayUsage() {
    echo "Usage: $0"
    echo "   -d [database name]"
    echo "   -i [Impala node e.g. impala-node:21000]"
    echo "   -b [HDFS path to Business Index directory e.g. hdfs:///businessindex/index]"
}

if [  $# -le 5 ]
then
    displayUsage
    exit 1
fi

DBNAME=""
IMPALADAEMON=""
BUSINESSINDEXPATH=""

while getopts ":d:i:b:" opt; do
    case "$opt" in
    d)
    DBNAME=$OPTARG
        ;;
    i)
    IMPALADAEMON=$OPTARG
        ;;
    b)
    BUSINESSINDEXPATH=$OPTARG
        ;;
    \?)
    echo "Invalid option: -$OPTARG" >&2
    exit 1
    ;;
    esac
done
shift $((OPTIND-1))

parquetFile=`hdfs dfs -ls ${BUSINESSINDEXPATH}/part* | head -1 | awk '{print $NF}'`

executeQuery='impala-shell -i '$IMPALADAEMON' -q'

createTableQuery="CREATE EXTERNAL TABLE $DBNAME.businessindex LIKE PARQUET '$parquetFile'
STORED AS PARQUET
LOCATION '$BUSINESSINDEXPATH'"

createViewQuery="CREATE VIEW $DBNAME.business_datasources AS 
SELECT businessindex.id, group_concat(sr.key) as sources
FROM $DBNAME.businessindex 
INNER JOIN $DBNAME.businessindex.sourcerecords sr
GROUP BY businessindex.id"

echo "Creating Impala table: $DBNAME.businessindex based on $parquetFile"
eval $executeQuery '"$createTableQuery"'

echo "Creating Impala view: $DBNAME.business_datasources"
eval $executeQuery '"$createViewQuery"'
