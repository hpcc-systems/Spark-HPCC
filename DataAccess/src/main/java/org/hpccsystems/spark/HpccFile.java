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
import java.util.ArrayList;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hpccsystems.spark.thor.FileFilter;
import org.hpccsystems.spark.thor.RemapInfo;
import org.hpccsystems.spark.thor.UnusableDataDefinitionException;
import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.ws.client.utils.Connection;

import org.apache.spark.sql.execution.python.EvaluatePython;

/**
 * Access to file content on a collection of one or more HPCC
 * clusters.
 *
 */
public class HpccFile implements Serializable {
  static private final long serialVersionUID = 1L;

  private HpccPart[] parts;
  private RecordDef recordDefinition;
  private boolean isIndex;

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
      RemapInfo remap_info, int maxParts) throws HpccFileException {
    this.recordDefinition = new RecordDef();  // missing, the default
    ColumnPruner cp = new ColumnPruner(targetColumnList);
    Connection conn = new Connection(protocol, host, port);
    conn.setUserName(user);
    conn.setPassword(pword);
    HPCCWsDFUClient hpcc = HPCCWsDFUClient.get(conn);
    String record_def_json = "";
    try {
      ArrayList<DFUFileDetailInfo> fd_list = new ArrayList<DFUFileDetailInfo>();
      HpccFile.recurseFDI(fd_list, fileName, hpcc);
      DFUFileDetailInfo[] fd_array = fd_list.toArray(new DFUFileDetailInfo[0]);
      this.isIndex = fd_array[0].isIndex();
      this.parts = HpccPart.makeFileParts(fd_array, remap_info, maxParts, filter);
      record_def_json = fd_array[0].getJsonInfo();
      if (record_def_json==null) {
        throw new UnusableDataDefinitionException("Definiiton returned was null");
      }
      this.recordDefinition = RecordDef.fromJsonDef(record_def_json, cp);
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
   * The partitions for the file residing on an HPCC cluster
   * @return
   * @throws HpccFileException
   */
  public HpccPart[] getFileParts() throws HpccFileException {
    HpccPart[] rslt = new HpccPart[parts.length];
    for (int i=0; i<parts.length; i++) rslt[i]=parts[i];
    return rslt;
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
    return new HpccRDD(sc, this.parts, this.recordDefinition);
  }
  /**
   * Make a Spark Dataframe (Dataset<Row>) of THOR data available.
   * @param session the Spark Session object
   * @return a Dataframe of THOR data
   * @throws HpccFileException when htere are errors reaching the THOR data.
   */
  public Dataset<Row> getDataframe(SparkSession session) throws HpccFileException{
    RecordDef rd = this.getRecordDefinition();
    HpccPart[] fp = this.getFileParts();
    JavaRDD<Row > rdd = (new HpccRDD(session.sparkContext(), fp, rd)).toJavaRDD();
    return session.createDataFrame(rdd, rd.asSchema());
  }
  /**
   * Is this an index?
   * @return true if yes
   */
  public boolean isIndex() { return this.isIndex; }
  /**
   * Recurse through the FileDetailInfo structure to get the list of actual files.
   * @param fd_list an ArrayList object to build up the list of real file
   * names
   * @param fileName the file name of interest, it may be a super file and if
   * so, we recursively call to resolve to a list of actual files
   * @param hpcc our connection to the wsclient services
   * @throws Exception thrown by wsclient services when something goes wrong
   */
  private static void recurseFDI(ArrayList<DFUFileDetailInfo> fd_list,
                                 String fileName,
                                 HPCCWsDFUClient hpcc) throws Exception {
    DFUFileDetailInfo fd = hpcc.getFileDetails(fileName, "", true, false);
    if (fd.getIsSuperfile()) {
      String[] subFileNames = fd.getSubfiles();
      for (int i=0; i<subFileNames.length; i++) {
        recurseFDI(fd_list, subFileNames[i], hpcc);
      }
    } else fd_list.add(fd);
  }
}
