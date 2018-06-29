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

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hpccsystems.spark.thor.ClusterRemapper;
import org.hpccsystems.spark.thor.FileFilter;
import org.hpccsystems.spark.thor.RemapInfo;
import org.hpccsystems.spark.thor.UnusableDataDefinitionException;
import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartsOnClusterInfo;
import org.hpccsystems.ws.client.utils.Connection;

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
      DFUFileDetailInfo fd = hpcc.getFileDetails(fileName, "", true, false);
      this.isIndex = fd.isIndex();
      this.parts = HpccPart.makeFileParts(fd, remap_info, maxParts, filter);
      record_def_json = fd.getJsonInfo();
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
    HpccDataframeFactory factory = new HpccDataframeFactory(session);
    return factory.getDataframe(this);
  }
  /**
   * Is this an index?
   * @return true if yes
   */
  public boolean isIndex() { return this.isIndex; }
}
