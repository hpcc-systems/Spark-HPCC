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
package org.hpccsystems.spark.thor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import org.hpccsystems.spark.HpccFileException;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartsOnClusterInfo;

/**
 * A partition of data.  One physical file
 * or key accessed by HPCC remote read.
 */
public class DataPartition implements Serializable {
  public static final long serialVersionUID = 1L;
  private String primary_ip;
  private String secondary_ip;
  private String file_name;
  private int this_part;
  private int num_parts;
  private int clearPort;
  private int sslPort;
  private long part_size;
  private boolean isCompressed;
  private boolean isIndex;
  private FileFilter fileFilter;
  /**
   * Construct the data part, used by makeParts
   * @param ip0 primary ip
   * @param ipx secondary ip
   * @param dir directory for file
   * @param this_part part number
   * @param num_parts number of parts
   * @param part_size size of this part
   * @param mask mask for constructing full file name
   * @param clear port number of clear communications
   * @param ssl port number of ssl communications
   * @param compressed_flag is the file compressed?
   * @param index_flag is this an index?
   * @param filter the file filter object
   */
  private DataPartition(String ip0, String ipx, String dir, int this_part,
      int num_parts, long part_size, String mask, int clear, int ssl,
      boolean compressed_flag, boolean index_flag, FileFilter filter) {
    String f_str = dir + "/" + mask;
    this.primary_ip = ip0;
    this.secondary_ip = ipx;
    this.file_name = f_str.replace("$P$", Integer.toString(this_part))
                          .replace("$N$", Integer.toString(num_parts));
    this.this_part = this_part;
    this.num_parts = num_parts;
    this.part_size = part_size;
    this.clearPort = clear;
    this.sslPort = ssl;
    this.isCompressed = compressed_flag;
    this.isIndex = index_flag;
    this.fileFilter = filter;
  }
  /**
   * Primary IP address
   * @return ip address
   */
  public String getPrimaryIP() { return this.primary_ip; }
  /**
   * Secondary IP for a copy.
   * @return ip address
   */
  public String getSecondaryIP() { return this.secondary_ip; }
  /**
   * Port used for communication in clear.
   * @return port number
   */
  public int getClearPort() { return clearPort; }
  /**
   * Port used for SSL communication
   * @return port
   */
  public int getSslPort() { return sslPort; }
  /**
   * File name
   * @return name
   */
  public String getFilename() {
    return this.file_name;
  }
  public int getThisPart() { return this.this_part; }
  /**
   * Number of parts for this file
   * @return number of parts
   */
  public int getNumParts() { return this.num_parts; }
  /**
   * Reported size of the file part on disk.
   * @return size
   */
  public long getPartSize() { return this.part_size; }
  /**
   * Is this a compressed file?
   * @return true when the dataset is compressed
   */
  public boolean isCompressed() {return this.isCompressed;}
  /**
   * Is the underlying file an index>
   * @return true if an index
   */
  public boolean isIndex() { return this.isIndex; }
  /**
   * The filter object to select specific rows
   * @return the filter object.
   */
  public FileFilter getFilter() { return this.fileFilter; }
  /*
   * (non-Javadoc)
   * @see java.lang.Object
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.getThisPart());
    sb.append(" ");
    sb.append(this.getPrimaryIP());
    sb.append(":");
    sb.append(this.getClearPort());
    sb.append(" ");
    sb.append(this.getFilename());
    return sb.toString();
  }
  /**
   * Make an array of data partitions for the supplied HPCC File
   * @param fdi the file detail information
   * @param remap_info used to remap the IP addresses or ports when the
   * THOR nodes are in a virtual cluster
   * @param max_parts the maximum number of parts or zero for no maximum
   * @param filters A filter using a list of fields each with one or more
   * ranges of values.
   * @return and array of partitions.
   * @throws HpccFileException
   */
  public static DataPartition[] createPartitions(DFUFileDetailInfo fdi,
      RemapInfo remap_info, int max_parts, FileFilter filter)
  throws HpccFileException {
    DFUFilePartsOnClusterInfo[] fp = fdi.getDFUFilePartsOnClusters();
    DFUFilePartInfo[] dfu_parts = fp[0].getDFUFileParts();  // always use first
    int num_content_parts = fdi.getNumParts() - ((fdi.isIndex()) ? 1  : 0);
    ClusterRemapper cr = ClusterRemapper.makeMapper(remap_info, dfu_parts);
    Arrays.sort(dfu_parts, FilePartInfoComparator);
    int copies = dfu_parts.length / fdi.getNumParts();
    int posSecondary = (copies==1) ? 0 : 1;
    DataPartition[] rslt = new DataPartition[num_content_parts];
    for (int i=0; i<num_content_parts; i++) {
      DFUFilePartInfo primary = dfu_parts[i * copies];
      DFUFilePartInfo secondary = dfu_parts[(i * copies) + posSecondary];
      rslt[i] = new DataPartition(cr.revisePrimaryIP(primary),
                          cr.reviseSecondaryIP(secondary),
                          fdi.getDir(), i+1, fdi.getNumParts(),
                          dfu_parts[i].getPartSizeInt64(),
                          fdi.getPathMask(), cr.reviseClearPort(primary),
                          cr.reviseSslPort(secondary), fdi.getIsCompressed(),
                          fdi.isIndex(), filter);
    }
    return rslt;
  }
  /**
   * Make an array of data partitions for the supplied HPCC File.
   * @param fdi the file detail information
   * @param max_parts the maximum number of partitions or zero for no limit
   * @param filter A filter using a list of fields each with one or more
   * value ranges
   * @return an array of partitions
   * @throws HpccFileException
   */
  public static DataPartition[] createPartitions(DFUFileDetailInfo fdi,
      int max_parts, FileFilter filter) throws HpccFileException {
    return createPartitions(fdi, new RemapInfo(), max_parts, filter);
  }
  /**
   * Make an array of data partitions for the supplied HPCC File.
   * @param fdi the file detail information
   * @param remap_info remap the IP or ports
   * @param max_parts the maximum number of partitions or zero for no limit
   * @return an array of partitions
   * @throws HpccFileException
   */
  public static DataPartition[] createPartitions(DFUFileDetailInfo fdi,
      RemapInfo remap_info, int max_parts) throws HpccFileException {
    return createPartitions(fdi, remap_info, max_parts, FileFilter.nullFilter());
  }
  /**
   * Comparator function to order file part information.
   */
  private static Comparator<DFUFilePartInfo> FilePartInfoComparator
                = new Comparator<DFUFilePartInfo>() {
    public int compare(DFUFilePartInfo fpi1, DFUFilePartInfo fpi2) {
      if (fpi1.getId() < fpi2.getId()) return -1;
      if (fpi1.getId() > fpi2.getId()) return 1;
      if (fpi1.getCopy() < fpi2.getCopy()) return -1;
      if (fpi1.getCopy() > fpi2.getCopy()) return 1;
      return 0;
    }
  };
}
