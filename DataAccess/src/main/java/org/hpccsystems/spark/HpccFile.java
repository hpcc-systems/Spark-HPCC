package org.hpccsystems.spark;

import org.hpccsystems.spark.thor.UnusableDataDefinitionException;
import org.hpccsystems.spark.thor.ClusterRemapper;
import org.hpccsystems.spark.thor.RemapInfo;
import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartsOnClusterInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;
import org.hpccsystems.ws.client.utils.Connection;
import java.io.Serializable;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

/**
 * Access to file content on a collection of one or more HPCC
 * clusters.
 * @author holtjd
 *
 */
public class HpccFile implements Serializable {
  static private final long serialVersionUID = 1L;

  private FilePart[] parts;
  private RecordDef recordDefinition;
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
   * @throws HpccFileException
   */
  public HpccFile(String fileName, String protocol, String host,
      String port, String user, String pword) throws HpccFileException{
    this(fileName, protocol, host, port, user, pword,
         new RemapInfo(0));
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
   * @param remap_info address and port re-mapping info for THOR cluster
   * @throws HpccFileException
   */
  public HpccFile(String fileName, String protocol, String host,
      String port, String user, String pword, RemapInfo remap_info)
      throws HpccFileException {
    this.recordDefinition = new RecordDef();  // missing, the default
    Connection conn = new Connection(protocol, host, port);
    conn.setUserName(user);
    conn.setPassword(pword);
    HPCCWsDFUClient hpcc = HPCCWsDFUClient.get(conn);
    try {
      DFUFileDetailInfo fd = hpcc.getFileDetails(fileName, "", true, false);
      DFUFilePartsOnClusterInfo[] fp = fd.getDFUFilePartsOnClusters();
      DFUFilePartInfo[] dfu_parts = fp[0].getDFUFileParts();
      ClusterRemapper cr = ClusterRemapper.makeMapper(remap_info, dfu_parts,
          fd.getNumParts());
      this.parts = FilePart.makeFileParts(fd.getNumParts(), fd.getDir(),
          fd.getFilename(), fd.getPathMask(), dfu_parts, cr, fd.getIsCompressed());
      String record_def_json = fd.getJsonInfo();
      if (record_def_json==null) {
        throw new UnusableDataDefinitionException("Definiiton returned was null");
      }
      this.recordDefinition = RecordDef.parseJsonDef(record_def_json);
    } catch (UnusableDataDefinitionException e) {
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
  public FilePart[] getFileParts() throws HpccFileException {
    FilePart[] rslt = new FilePart[parts.length];
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
}
