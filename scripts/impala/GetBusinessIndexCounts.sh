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

queryOptions[0]="-c"
queryOptions[1]="-c -v"
queryOptions[2]="-c -v -p"
queryOptions[3]="-c -p"
queryOptions[4]="-v"
queryOptions[5]="-v -p"
queryOptions[6]="-p"

tradingStatus[0]="ACTIVE"
tradingStatus[1]="CLOSED"
tradingStatus[2]="INSOLVENT"

function buildCountConditions {
    local companiesHouseCondition="NOT LIKE '%CompaniesHouse%'"
    local vatCondition="NOT LIKE '%Vat%'"
    local payeCondition="NOT LIKE '%Paye%'"

    description=""

    local OPTIND
    while getopts "cvp" opt; do
        case "$opt" in
        c)
          companiesHouseCondition="LIKE '%CompaniesHouse%'"
          description="$description Companies House"
          ;;
        v)
          vatCondition="LIKE '%Vat%'"
          description="$description VAT"
          ;;
        p)
          payeCondition="LIKE '%Paye%'"
          description="$description Paye"
          ;;
        \?)
        echo "Invalid option: -$OPTARG" >&2
        exit 1
        ;;
        esac
    done
    shift $((OPTIND-1))

    return ("$description: " "business_datasources.sources $companiesHouseCondition
      AND business_datasources.sources $vatCondition
      AND business_datasources.sources $payeCondition")
}

function executeCountQuery {
  local fromClause=$0
  local whereClause=$1
  return $(impala-shell -B --quiet -i '$IMPALADAEMON' -q "SELECT count(*) FROM $fromClause WHERE $whereClause")
}

function getCountsForTradingStatus {
  countTradingStatusOutput=""
  local fromClause="datasources_for_ids
    LEFT JOIN
    ( SELECT id, sr.tradingstatus from businessindex, businessindex.sourcerecords sr WHERE sr.datasource = 'CompaniesHouse'
     ) AS ch 
    ON ch.id = ids.id
    LEFT JOIN
    ( SELECT id, sr.tradingstatus from businessindex, businessindex.sourcerecords sr WHERE sr.datasource = 'Vat'
     ) AS vat
    ON vat.id = ids.id
    LEFT JOIN
    ( SELECT id, sr.tradingstatus from businessindex, businessindex.sourcerecords sr WHERE sr.datasource = 'Paye' 
     ) AS paye
    ON paye.id = ids.id"

  for status in "${tradingStatus[@]}"
  do
    countTradingStatusOutput="$countTradingStatusOutput \n === Trading Status: $status === \n"

    for option in "${queryOptions[@]}"
    do
      whereClauseAndDescription=$(buildCountConditions $option)
      tradingStatusWhereClause="$whereClauseAndDescription[1] 
        AND and coalesce(ch.tradingstatus, vat.tradingstatus, paye.tradingstatus) = '$status'"
      countTradingStatusOutput="$countTradingStatusOutput $whereClauseAndDescription[0]:"
      countTradingStatusOutput="$countTradingStatusOutput $(executeCountQuery $fromClause $tradingStatusWhereClause) \n"
    done
  done

  return $countTradingStatusOutput
}

function getCountsForSourceTypes {
  countOutput="=== SUMMARY === \n"
  local fromClause="business_datasources"

  for option in "${queryOptions[@]}"
  do
    whereClauseAndDescription=$(buildCountConditions $option)
    countOutput="$countOutput $whereClauseAndDescription[0]:"
    countOutput="$countOutput $(executeCountQuery $fromClause $whereClauseAndDescription[1]) \n"
  done

  return $countOutput
}

function getTotalCounts {
  totalCountOutput="Total Business Index:"

  totalCountQuery="SELECT count(*) FROM $DBNAME.business_datasources"
  totalCountOutput="$totalCountOutput $(impala-shell -B --quiet -i '$IMPALADAEMON' -q "$totalCountQuery") \n"

  return $totalCountOutput 
}
 
echo getCountsForTradingStatus
echo getCountsForSourceTypes