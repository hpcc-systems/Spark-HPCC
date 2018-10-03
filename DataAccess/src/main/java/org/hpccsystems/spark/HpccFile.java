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
import java.util.UUID;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hpccsystems.spark.thor.ClusterRemapper;
import org.hpccsystems.spark.thor.DataPartition;
import org.hpccsystems.spark.thor.FileFilter;
import org.hpccsystems.spark.thor.RemapInfo;
import org.hpccsystems.spark.thor.UnusableDataDefinitionException;
import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.gen.wsdfu.v1_39.SecAccessType;
import org.hpccsystems.ws.client.utils.Connection;
import org.hpccsystems.ws.client.wrappers.wsdfu.DFUFileAccessInfoWrapper;
import org.apache.spark.sql.execution.python.EvaluatePython;

/**
 * Access to file content on a collection of one or more HPCC
 * clusters.
 *
 */
public class HpccFile implements Serializable {
  static private final long serialVersionUID = 1L;

  private DataPartition[] dataParts;
  private RecordDef recordDefinition;
  private boolean isIndex;
  static private final int DEFAULT_ACCESS_EXPIRY_SECONDS = 120;
  private int fileAccessExpirySecs = DEFAULT_ACCESS_EXPIRY_SECONDS;

  // Make sure Python picklers have been registered
  static {
      EvaluatePython.registerPicklers();
  }

  /**
   * Constructor for the HpccFile.  Captures the information
   * from the DALI Server for the
   * clusters behind the ESP named by the IP address.
   * @param fileName The HPCC file name
   * @param protocol usually http or https
   * @param host the ESP address
   * @param port the ESP port
   * @param user a valid account that has access to the file
   * @param pword a valid pass word for the account
   * @param targetColumnList a comma separated list of column names with dotted
   * names
   * @throws HpccFileException
   */
  public HpccFile(String fileName, String protocol, String host, String port,
      String user, String pword, String targetColumnList)
          throws HpccFileException{
    this(fileName, protocol, host, port, user, pword, targetColumnList,
        FileFilter.nullFilter(), new RemapInfo(), 0);
  }
 /**
   * Constructor for the HpccFile.  Captures the information
   * from the DALI Server for the
   * clusters behind the ESP named by the IP address.
   * @param fileName The HPCC file name
   * @param protocol usually http or https
   * @param host the ESP address
   * @param port the ESP port
   * @param user a valid account that has access to the file
   * @param pword a valid pass word for the account
   * @param targetColumnList a comma separated list of column names with dotted
   * names
   * @param maxParts the maximum number of partitions to use or zero if no max
   * @throws HpccFileException
   */
  public HpccFile(String fileName, String protocol, String host, String port,
          String user, String pword, String targetColumnList, int maxParts)
          throws HpccFileException{
    this(fileName, protocol, host, port, user, pword, targetColumnList,
        FileFilter.nullFilter(), new RemapInfo(), maxParts);
  }
  /**
   * Constructor for the HpccFile.  Captures the information
   * from the DALI Server for the
   * clusters behind the ESP named by the IP address.
   * @param fileName The HPCC file name
   * @param protocol usually http or https
   * @param host the ESP address
   * @param port the ESP port
   * @param user a valid account that has access to the file
   * @param pword a valid pass word for the account
   * @param targetColumnList a comma separated list of colomn names with dotted names
   * @param filter a file filter to select a subset of records
   * @throws HpccFileException
   */
  public HpccFile(String fileName, String protocol, String host,
      String port, String user, String pword, String targetColumnList,
      FileFilter filter) throws HpccFileException {
    this(fileName, protocol, host, port, user, pword, targetColumnList,
         filter, new RemapInfo(), 0);
  }
  /**
   * Constructor for the HpccFile.  Captures the information
   * from the DALI Server for the
   * clusters behind the ESP named by the IP address.
   * @param fileName The HPCC file name
   * @param protocol usually http or https
   * @param host the ESP address
   * @param port the ESP port
   * @param user a valid account that has access to the file
   * @param pword a valid pass word for the account
   * @param targetColumnList a comma separated list of colomn names with dotted names
   * @param filter a file filter to select a subset of records
   * @param maxParts the maximum number of partitions or zero for no max
   * @throws HpccFileException
   */
  public HpccFile(String fileName, String protocol, String host,
      String port, String user, String pword, String targetColumnList,
      FileFilter filter, int maxParts) throws HpccFileException {
    this(fileName, protocol, host, port, user, pword, targetColumnList,
         filter, new RemapInfo(), maxParts);
  }
  /**
   * Constructor for the HpccFile.  Captures the information
   * from the DALI Server for the
   * clusters behind the ESP named by the IP address.
   * @param fileName The HPCC file name
   * @param protocol usually http or https
   * @param host the ESP address
   * @param port the ESP port
   * @param user a valid account that has access to the file
   * @param pword a valid pass word for the account
   * @param targetColumnList a comma separated list of colomn names with dotted names
   * @param remap_info address and port re-mapping info for THOR cluster
   * @throws HpccFileException
   */
  public HpccFile(String fileName, String protocol, String host,
      String port, String user, String pword, String targetColumnList,
      RemapInfo remap_info) throws HpccFileException {
    this(fileName, protocol, host, port, user, pword, targetColumnList,
         FileFilter.nullFilter(), remap_info, 0);
  }
  /**
   * Constructor for the HpccFile.  Captures the information
   * from the DALI Server for the
   * clusters behind the ESP named by the IP address.
   * @param fileName The HPCC file name
   * @param protocol usually http or https
   * @param host the ESP address
   * @param port the ESP port
   * @param user a valid account that has access to the file
   * @param pword a valid pass word for the account
   * @param targetColumnList a comma separated list of colomn names with dotted names
   * @param remap_info address and port re-mapping info for THOR cluster
   * @param maxParts the maximum number of partitions or zero for no max
   * @throws HpccFileException
   */
  public HpccFile(String fileName, String protocol, String host,
      String port, String user, String pword, String targetColumnList,
      RemapInfo remap_info, int maxParts) throws HpccFileException {
    this(fileName, protocol, host, port, user, pword, targetColumnList,
         FileFilter.nullFilter(), remap_info, maxParts);
  }

  /**
   * Constructor for the HpccFile.  Captures the information
   * from the DALI Server for the
   * clusters behind the ESP named by the IP address and re-maps
   * the address information for the THOR nodes to visible addresses
   * when the THOR clusters are virtual.
   * @param fileName The HPCC file name
   * @param protocol usually http or https
   * @param host the ESP address
   * @param port the ESP port
   * @param user a valid account that has access to the file
   * @param pword a valid pass word for the account
   * @param targetColumnList a comma separated list of column names in dotted
   * notation for columns within compound columns.
   * @param filter a file filter to select records of interest
   * @param remap_info address and port re-mapping info for THOR cluster
   * @param maxParts the maximum number of partitions or zero for no max
   * @throws HpccFileException
   */
  public HpccFile(String fileName, String protocol, String host, String port,
      String user, String pword, String targetColumnList, FileFilter filter,
      RemapInfo remap_info, int maxParts) throws HpccFileException
  {
    this.recordDefinition = new RecordDef();  // missing, the default
    ColumnPruner cp = new ColumnPruner(targetColumnList);
    Connection conn = new Connection(protocol, host, port);
    conn.setUserName(user);
    conn.setPassword(pword);
    HPCCWsDFUClient dfuClient = HPCCWsDFUClient.get(conn);
    String record_def_json = "";
    try {
      DFUFileAccessInfoWrapper fileinfoforread = fetchReadFileInfo(fileName, dfuClient, fileAccessExpirySecs, "");
      if (fileinfoforread.getNumParts() > 0)
      {
          ClusterRemapper clusterremapper = ClusterRemapper.makeMapper(remap_info, fileinfoforread.getAllFilePartCopyLocations());
          this.dataParts = DataPartition.createPartitions(fileinfoforread.getFileParts(), clusterremapper, maxParts, filter, fileinfoforread.getFileAccessInfoBlob());
          record_def_json = fileinfoforread.getRecordTypeInfoJson();
          if (record_def_json==null)
          {
              throw new UnusableDataDefinitionException("Definiton returned was null");
          }
          this.recordDefinition = RecordDef.fromJsonDef(record_def_json, cp);
      }
      else
          throw new HpccFileException("Could not fetch metadata for file: '" + fileName + "'");

    } catch (UnusableDataDefinitionException e) {
      System.err.println(record_def_json);
      throw new HpccFileException("Bad definition", e);
    } catch (Exception e) {
      StringBuilder sb = new StringBuilder();
      sb.append("Failed to access file ");
      sb.append(fileName);
      throw new HpccFileException(sb.toString(), e);
    }
  }

  /**
   * Constructor for HpccFile.
   * Captures the information from the DALI Server for the
   * clusters behind the ESP named by the IP address and re-maps
   * the address information for the THOR nodes to visible addresses
   * when the THOR clusters are virtual.
   *
   * @param fileName The HPCC file name
   * @param protocol usually http or https
   * @param host the ESP address
   * @param port the ESP port
   * @param user a valid account that has access to the file
   * @param pword a valid pass word for the account
   * @param targetColumnList a comma separated list of column names in dotted
   *        notation for columns within compound columns.
   * @param filter a file filter to select records of interest
   * @param remap_info address and port re-mapping info for THOR cluster
   * @param maxParts the maximum number of partitions or zero for no max
   * @param fileAccessExpirySecs initial access to a file is granted for a period
   *        of time. This param can change the duration of that file access.
   * @throws HpccFileException
   */
  public HpccFile(String fileName, String protocol, String host, String port,
      String user, String pword, String targetColumnList, FileFilter filter,
      RemapInfo remap_info, int maxParts, int fileAccessExpirySecs) throws HpccFileException
  {
  this(fileName, protocol, host, port, user, pword, targetColumnList, filter, remap_info, maxParts);
  this.fileAccessExpirySecs = fileAccessExpirySecs;
  }
  /**
   * The partitions for the file residing on an HPCC cluster
   * @return
   * @throws HpccFileException
   */
  public DataPartition[] getFileParts() throws HpccFileException
  {
      return dataParts;
  }
  /**
   * The record definition for a file on an HPCC cluster.
   * @return
   * @throws HpccFileException
   */
  public RecordDef getRecordDefinition() throws HpccFileException {
    return recordDefinition;
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
	  return new HpccRDD(sc, this.dataParts, this.recordDefinition);
  }
  /**
   * Make a Spark Dataframe (Dataset<Row>) of THOR data available.
   * @param session the Spark Session object
   * @return a Dataframe of THOR data
   * @throws HpccFileException when htere are errors reaching the THOR data.
   */
  public Dataset<Row> getDataframe(SparkSession session) throws HpccFileException{
    RecordDef rd = this.getRecordDefinition();
    DataPartition[] fp = this.getFileParts();
    JavaRDD<Row > rdd = (new HpccRDD(session.sparkContext(), fp, rd)).toJavaRDD();
    return session.createDataFrame(rdd, rd.asSchema());
  }
  /**
   * Is this an index?
   * @return true if yes
   */
  public boolean isIndex() { return this.isIndex; }

  private static  DFUFileAccessInfoWrapper fetchReadFileInfo(String fileName, HPCCWsDFUClient hpccClient, int expirySeconds, String clusterName) throws Exception
  {
    String uniqueID = "SPARK-HPCC: " + UUID.randomUUID().toString();
    return hpccClient.getFileAccess(SecAccessType.Read, fileName, clusterName, expirySeconds, uniqueID, true, false, true);
  }

  private static String acquireReadFileAccess(String fileName, HPCCWsDFUClient hpccClient, int expirySeconds, String clusterName) throws Exception
  {
    return acquireFileAccess(fileName, SecAccessType.Read, hpccClient, expirySeconds, clusterName);
  }

  private static String acquireWriteFileAccess(String fileName, HPCCWsDFUClient hpccClient, int expirySeconds, String clusterName) throws Exception
  {
    return acquireFileAccess(fileName, SecAccessType.Write, hpccClient, expirySeconds, clusterName);
  }

  private static String acquireFileAccess(String fileName, SecAccessType accesstype, HPCCWsDFUClient hpcc, int expirySeconds, String clusterName) throws Exception
  {
    String uniqueID = "SPARK-HPCC: " + UUID.randomUUID().toString();
    return hpcc.getFileAccessBlob(accesstype, fileName, clusterName, expirySeconds, uniqueID);
  }
}
