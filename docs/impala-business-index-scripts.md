# Script: CreateBusinessIndexTable.sh

## How to run it

Bash script used in an environment which has access to the `impala-shell` command.

`CreateBusinessIndexTable.sh` supports the following arguments:
- `-d` - the Impala/Hive database name to create the `businessindex` table in (required)
- `-i` - the Impala deamon to connect to (required)
- `-b` - the business index path in HDFS (required)

### How to run it in an Impala cluster

```
./CreateBusinessIndexTable -d bi -i impala-node:21000 -b "hdfs:///tmp/businessindex"
```

## Output

The following objects are crated in Hive/Impala as a result of the script:
- `businessindex` - The parquet formatted business index table
- `business_datasources` - aggregate table mapping business id's to the datasource names which are used to create it.
	- e.g. "1232345" -> "Vat, CompaniesHouse, Paye"

# Script: GetBusinessIndexCounts.sh

## How to run it

Bash script used in an environment which has access to the `impala-shell` command.

`GetBusinessIndexCounts.sh` supports the following arguments:
- `-d` - the Impala/Hive database name where the `businessindex` table exists (required)
- `-i` - the Impala deamon to connect to (required)

### How to run it in an Impala cluster

```
./GetBusinessIndexCounts -d bi -i impala-node:21000
```

## Output

Produces counts based on the `business_datasources` view, split by `Trading Status`. The output shows how many of records from each datasource were used to make the Business Index record. A summary row is included:
```
=== Trading Status: ACTIVE ===
Total Business Index: 3
Companies House only: 0
Companies House and VAT only: 0
Companies House and Paye only: 2
Companies House and VAT and Paye: 0
VAT only: 1
VAT and Paye only: 0
Paye only: 0

=== Trading Status: SUMMARY ===
...
...

```