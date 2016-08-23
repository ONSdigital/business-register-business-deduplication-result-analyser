# Audit log comparator

This tool compares results of two de-duplication processes and prints the differences into CSV file.

# How to run it

Project main class is used to run comparison. The following arguments are supported:

- `--previousAuditLogPath` - path to audit log directory created by first de-duplication process (required)
- `--newAuditLogPath` - path to audit log directory created by second de-duplication process (required)
- `--outputFile` - path to output file which will contain differences in process results (required)

## How to run it locally

Comparison process can be launched locally by executing following command:

```
sbt "run <arguments>"
```

For example following commands might be used to compare audit files from `/tmp/output/business-index-20160711/audit-log` and `/tmp/output/business-index-20160712/audit-log` directories.

```
sbt "run --previousAuditLogPath /tmp/output/business-index-20160711/audit-log
         --newAuditLogPath /tmp/output/business-index-20160712/audit-log
         --outputFile /tmp/output/business-index-diff.csv
```

```
spark-submit --master local[*]
             --packages com.github.scopt:scopt_2.10:3.5.0,com.github.tototoshi:scala-csv_2.10:1.3.3
             .target/scala-2.10/business-deduplication-result-analyser_2.10-1.0.0-SNAPSHOT.jar
             --previousAuditLogPath /tmp/output/business-index-20160711/audit-log
             --newAuditLogPath /tmp/output/business-index-20160712/audit-log
             --outputFile /tmp/output/business-index-diff.csv
```

In above scenario `business-index-diff.csv` file  (diff file) would be created in `/tmp/output/` directory.

# Output format

Comparison output is created in form of CSV document with following columns:

 - source data from audit log in columnar format (at the moment that is 8 fields from the data record)
 - change between two de-duplication runs in text format (_'category from first run => category from second run'_)
 - list of business records from first de-duplication run in JSON format
 - list of business records from second de-duplication run in JSON format