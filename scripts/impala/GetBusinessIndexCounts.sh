#!/bin/bash
##
#
# Impala script to get counts of each data source merge
#
# Should be passed a -d argument for the database name where the businessindex table exists
# Should be passed a -i argument for the Impala deamon to connect to
#
##

OPTIND=1

displayUsage() {
    echo "Usage: $0"
    echo "	-d [database name]"
    echo "	-i [Impala node e.g. impala-node:21000]"
}

if [  $# -le 3 ]
then
    displayUsage
    exit 1
fi

CH="CompaniesHouse"
VAT="Vat"
PAYE="Paye"

DBNAME=""
IMPALADAEMON=""

while getopts ":d:i:" opt; do
    case "$opt" in
    d)
    DBNAME=$OPTARG
        ;;
    i)
    IMPALADAEMON=$OPTARG
        ;;
    \?)
    echo "Invalid option: -$OPTARG" >&2
    exit 1
    ;;
    esac
done
shift $((OPTIND-1))

getCount='impala-shell -B --quiet -i '$IMPALADAEMON' -q'

totalCountQuery="SELECT count(*) FROM $DBNAME.business_datasources ids"

chQuery="SELECT count(*) FROM $DBNAME.business_datasources ids
WHERE ids.sources LIKE '%$CH%' 
AND ids.sources NOT LIKE '%$VAT%' 
AND ids.sources NOT LIKE '%$PAYE%'"

chVatQuery="SELECT count(*) FROM $DBNAME.business_datasources ids
WHERE ids.sources LIKE '%$CH%' 
AND ids.sources LIKE '%$VAT%' 
AND ids.sources NOT LIKE '%$PAYE%'"

chPayeQuery="SELECT count(*) FROM $DBNAME.business_datasources ids
WHERE ids.sources LIKE '%$CH%' 
AND ids.sources LIKE '%$PAYE%' 
AND ids.sources NOT LIKE '%$VAT%'"

chVatPayeQuery="SELECT count(*) FROM $DBNAME.business_datasources ids
WHERE ids.sources LIKE '%$CH%' 
AND ids.sources LIKE '%$PAYE%' 
AND ids.sources LIKE '%$VAT%'"

vatQuery="SELECT count(*) FROM $DBNAME.business_datasources ids
WHERE ids.sources LIKE '%$VAT%' 
AND ids.sources NOT LIKE '%$PAYE%' 
AND ids.sources NOT LIKE '%$CH%'"

vatPayeQuery="SELECT count(*) FROM $DBNAME.business_datasources ids
WHERE ids.sources LIKE '%$VAT%' 
AND ids.sources LIKE '%$PAYE%' 
AND ids.sources NOT LIKE '%$CH%'"

payeQuery="SELECT count(*) FROM $DBNAME.business_datasources ids
WHERE ids.sources LIKE '%$PAYE%' 
AND ids.sources NOT LIKE '%$VAT%' 
AND ids.sources NOT LIKE '%$CH%'"

totalCount=$($getCount "$totalCountQuery" -r)
ch=$($getCount "$chQuery")
chVat=$($getCount "$chVatQuery")
chPaye=$($getCount "$chPayeQuery")
chVatPaye=$($getCount "$chVatPayeQuery")
vat=$($getCount "$vatQuery")
vatPaye=$($getCount "$vatPayeQuery")
paye=$($getCount "$payeQuery")

echo "Total Business Index: ${totalCount//[$'\n']/}"
echo "Companies House only: $ch"
echo "Companies House and VAT only: $chVat"
echo "Companies House and Paye only: $chPaye"
echo "Companies House and VAT and Paye: $chVatPaye"
echo "VAT only: $vat"
echo "VAT and Paye only: $vatPaye"
echo "Paye only: $paye"

