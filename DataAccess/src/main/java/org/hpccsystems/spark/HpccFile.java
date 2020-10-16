/*******************************************************************************
 *     HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *******************************************************************************/
package org.hpccsystems.spark;

import java.io.Serializable;
import java.net.MalformedURLException;


import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.execution.python.EvaluatePython;
import org.hpccsystems.dfs.cluster.RemapInfo;
import org.hpccsystems.dfs.client.DataPartition;
import org.hpccsystems.commons.ecl.FieldDef;
import org.hpccsystems.commons.errors.HpccFileException;
import org.hpccsystems.ws.client.utils.Connection;

/**
 * Access to file content on a collection of one or more HPCC
 * clusters.
 *
 */
public class HpccFile extends org.hpccsystems.dfs.client.HPCCFile implements Serializable {
  static private final long serialVersionUID = 1L;

  private int recordLimit = -1;

  // Make sure Python picklers have been registered
  static { EvaluatePython.registerPicklers(); }

  /**
   * Constructor for the HpccFile.
   * Captures HPCC logical file  information from the DALI Server
   * for the clusters behind the ESP named by the Connection.
   *
   * @param fileName The HPCC file name
   * @param espconninfo The ESP connection info (protocol,address,port,user,pass)
   * @throws HpccFileException hpcc file exection
   */
  public HpccFile(String fileName, Connection espconninfo) throws HpccFileException
  {
    super(fileName,espconninfo);
  }

  /**
   * Constructor for the HpccFile.
   * Captures HPCC logical file  information from the DALI Server
   * for the clusters behind the ESP named by the Connection.
   *
   * @param fileName The HPCC file name
   * @param connectionString to eclwatch. Format: {http|https}://{HOST}:{PORT}.
   * @param user username
   * @param pass password
   * @throws MalformedURLException Malformed URL exception
   * @throws HpccFileException hpcc file exception
   */
  public HpccFile(String fileName, String connectionString, String user, String pass) throws MalformedURLException, HpccFileException
  {
    super(fileName,connectionString,user,pass);
  }
  /**
   * Constructor for the HpccFile.
   * Captures HPCC logical file information from the DALI Server for the
   * clusters behind the ESP named by the IP address and re-maps
   * the address information for the THOR nodes to visible addresses
   * when the THOR clusters are virtual.
   * @param fileName The HPCC file name
   * @param espconninfo esp connection information object
   * @param targetColumnList a comma separated list of column names in dotted
   * notation for columns within compound columns.
   * @param filter a file filter to select records of interest
   * @param remap_info address and port re-mapping info for THOR cluster
   * @param maxParts optional the maximum number of partitions or zero for no max
   * @param targetfilecluster optional - the hpcc cluster the target file resides in
   * @throws HpccFileException hpcc file exception
   */
  public HpccFile(String fileName, Connection espconninfo, String targetColumnList, String filter, RemapInfo remap_info, int maxParts, String targetfilecluster) throws HpccFileException
  {
    super(fileName,espconninfo,targetColumnList,filter,remap_info,maxParts,targetfilecluster);
  }

  /**
   * Set file part record limit
   * @param limit fire part record limit
   */
  public void setFilePartRecordLimit(int limit)
  {
    this.recordLimit = limit;
  }

  /**
   * Returns the current file part record limit
   * @return returns file part record limit
   */
  public int getFilePartRecordLimit()
  {
    return this.recordLimit;
  }

  /**
   * Make a Spark Resilient Distributed Dataset (RDD) that provides access
   * to THOR based datasets. Uses existing SparkContext, allows this function
   * to be used from PySpark.
   * @return An RDD of THOR data.
   * @throws HpccFileException When there are errors reaching the THOR data
   */
  public HpccRDD getRDD() throws HpccFileException {
    return getRDD(SparkContext.getOrCreate());
  }
  /**
   * Make a Spark Resilient Distributed Dataset (RDD) that provides access
   * to THOR based datasets.
   * @param sc Spark Context
   * @return An RDD of THOR data.
   * @throws HpccFileException When there are errors reaching the THOR data
   */
  public HpccRDD getRDD(SparkContext sc) throws HpccFileException {
	  return new HpccRDD(sc, getFileParts(), this.getRecordDefinition(), this.getProjectedRecordDefinition(), this.getFileAccessExpirySecs(), this.recordLimit);
  }
  /**
   * Make a Spark Dataframe (Dataset (Row)) of THOR data available.
   * @param session the Spark Session object
   * @return a Dataframe of THOR data
   * @throws HpccFileException when htere are errors reaching the THOR data.
   */
  public Dataset<Row> getDataframe(SparkSession session) throws HpccFileException{
    FieldDef originalRD = this.getRecordDefinition();
    FieldDef projectedRD = this.getProjectedRecordDefinition();
    DataPartition[] fp = this.getFileParts();
    JavaRDD<Row > rdd = (new HpccRDD(session.sparkContext(), fp, originalRD, projectedRD, this.getFileAccessExpirySecs(), this.recordLimit)).toJavaRDD();

    StructType schema = null;
    try {
      schema = SparkSchemaTranslator.toSparkSchema(projectedRD);
    } catch(Exception e) {
      throw new HpccFileException(e.getMessage());
    }

    return session.createDataFrame(rdd, schema);
  }
}
