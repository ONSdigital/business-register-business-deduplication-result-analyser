# Overview

The purpose of this project is to provide set of tools to analyse de-duplication audit log. At the moment following tools are available:

- comparator to find differences between results of two de-duplication processes and presents them in CSV file
- scripts for [Impala](https://www.cloudera.com/documentation/enterprise/5-5-x/topics/impala.html) to create and query the business index

# Requirements

Requirements mentioned below are optional as long as analysis tools are run locally. To use analysis tools remotely please ensure following requirements are met.

## Software (optional)

- Apache Spark (http://spark.apache.org) installed via Homebrew (http://brew.sh)
- ONS development environment installed (https://github.com/ONSdigital/ons-devops/tree/master/ons-business-dev)
	- Note: Impala has not been added to this environment yet preventing the Impala scripts from being used

## Environment variables (optional)

To run analysis in ONS development environment the following environment variables must be set:

```
export DEV_ENVIRONMENT_DIR=<path to ons-devops repository>/ons-business-dev

export HADOOP_USER_NAME=vagrant
export HADOOP_CONF_DIR=$DEV_ENVIRONMENT_DIR/provisioning/roles/apache-hadoop/templates/opt/apache-hadoop/etc/hadoop
```

# How to build it

Project can be built using either `sbt` installed locally or `bin/sbt` script. First approach will be used in all examples presented here.

## Cross project dependencies
This project depends on the following projects being built and published:

- ["uk.gov.ons.business-register" %% "test-utils" % "1.0.0-SNAPSHOT" % "test"](https://github.com/ONSdigital/business-register-business-libs)

## How to build it without dependencies

To build the project please execute following command:

```
sbt package
```

Above command will create `business-deduplication-result-analyser_2.10-1.0.0-SNAPSHOT.jar` JAR file in `target/scala-2.10` directory.

## How to build it with dependencies

To build the project with all runtime dependencies (uber-jar) please execute following command:

```
sbt assembly
```

Above command will create `business-deduplication-result-analyser-assembly-1.0.0-SNAPSHOT.jar` JAR file in `target/scala-2.10` directory.

## How to compare de-duplication audit logs

Process of comparing de-duplication audit logs is described [here](docs/audit-log-comparator.md).

## How to use the Impala business index scripts

Process of creating the Impala business index tables and executing queries is described [here](docs/impala-business-index-scripts.md).
