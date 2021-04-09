![HPCC Spark Connector Nightly](https://github.com/hpcc-systems/spark-hpcc/workflows/HPCC%20Spark%20Connector%20Nightly/badge.svg)
[![javadoc](https://javadoc-badge.appspot.com/org.hpccsystems/spark-hpcc.svg?label=javadoc)](https://javadoc-badge.appspot.com/org.hpccsystems/spark-hpcc)

##### Current Maven Central Releases:
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.hpccsystems/spark-hpcc/badge.svg?subject=spark-hpcc)](https://maven-badges.herokuapp.com/maven-central/org.hpccsystems/spark-hpcc)

# Spark-HPCC
Spark classes for HPCC Systems/ Spark interoperability

This repository is comprised of two projects, DataAccess and Examples.

### DataAccess
The DataAccess project contains the classes which expose distributed
streaming of HPCC based data via Spark constructs. In addition,
the HPCC data is exposed as a Dataframe for the convenience of the Spark developer.

#### Dependancies
The spark-hpcc target jar does not package any of the Spark libraries it depends on.
If using a standard Spark submission pipeline such as spark-submit these dependencies will be provided as part of the Spark installation.
However, if your pipeline executes a jar directly you may need to add the Spark libraries from your $SPARK_HOME to the classpath.

### Examples
The Examples project contains examples in Scala for
using HPCC THOR cluster based data in a Machine
Learning application.

## Please note:
##### As reported by github:

"In all versions of Apache Spark, its standalone resource manager accepts code to execute on a 'master' host, that then runs that code on 'worker' hosts. The master itself does not, by design, execute user code. A specially-crafted request to the master can, however, cause the master to execute code too. Note that this does not affect standalone clusters with authentication enabled. While the master host typically has less outbound access to other resources than a worker, the execution of code on the master is nevertheless unexpected.
Mitigation

Enable authentication on any Spark standalone cluster that is not otherwise secured from unwanted access, for example by network-level restrictions. Use spark.authenticate and related security properties described at https://spark.apache.org/docs/latest/security.html"

