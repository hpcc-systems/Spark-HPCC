![HPCC Spark Connector Nightly](https://github.com/hpcc-systems/spark-hpcc/workflows/HPCC%20Spark%20Connector%20Nightly/badge.svg)

# Spark-HPCC
Spark classes for working with HPCC clusters

There are two projects, DataAccess and Examples.

The DataAccess project contains the classes to support
reading data from a THOR cluster with a Spark RDD.  In
addition, te HPCC data is exposed as a Dataframe for
the convenience of the Spark developer.

The Examples project contains examples in Scala for
using HPCC THOR cluster based data in a Machine
Learning application.

Please note:
As reported by github: 
"In all versions of Apache Spark, its standalone resource manager accepts code to execute on a 'master' host, that then runs that code on 'worker' hosts. The master itself does not, by design, execute user code. A specially-crafted request to the master can, however, cause the master to execute code too. Note that this does not affect standalone clusters with authentication enabled. While the master host typically has less outbound access to other resources than a worker, the execution of code on the master is nevertheless unexpected.
Mitigation

Enable authentication on any Spark standalone cluster that is not otherwise secured from unwanted access, for example by network-level restrictions. Use spark.authenticate and related security properties described at https://spark.apache.org/docs/latest/security.html"

