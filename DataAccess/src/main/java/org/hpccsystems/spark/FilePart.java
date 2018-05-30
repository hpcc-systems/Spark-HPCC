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
/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.Partition;
import org.hpccsystems.spark.thor.ClusterRemapper;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;

/**
 * A file part of an HPCC file.  This is the Spark partition for the RDD.
 *
 */
public class FilePart implements Partition, Serializable {
  static private final long serialVersionUID = 1L;
  static private NumberFormat fmt = NumberFormat.getInstance();
  private String primary_ip;
  private String secondary_ip;
  private String file_name;
  private int this_part;
  private int num_parts;
  private int clearPort;
  private int sslPort;
  private long part_size;
  private boolean isCompressed;

  /**
   * Construct the file part, used by makeParts
   * @param ip0 primary ip
   * @param ipx secondary ip
   * @param dir directory for file
   * @param name file name
   * @param this_part part number
   * @param num_parts number of parts
   * @param part_size size of this part
   * @param mask mask for constructing full file name
   * @param clear port number of clear communications
   * @param ssl port number of ssl communications
   * @param compressed_flag is the file compressed?
   */
  private FilePart(String ip0, String ipx, String dir, String name,
      int this_part, int num_parts, long part_size, String mask,
      int clear, int ssl, boolean compressed_flag) {
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
  }
  /**
   * Empty constructor used by serialization
   */
  protected FilePart() {}

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
  /* (non-Javadoc)
   * @see org.apache.spark.Partition#index()
   */
  public int index() {
    return this_part - 1;
  }
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
  /* (non-Javadoc)
   * Spark core 2.10 needs this defined, not needed in 2.11
   */
  public boolean org$apache$spark$Partition$$super$equals(Object arg0) {
    if (!(arg0 instanceof FilePart)) return false;
    FilePart fp0 = (FilePart) arg0;
    if (!this.getFilename().equals(fp0.getFilename())) return false;
    if (this.getNumParts() != fp0.getNumParts()) return false;
    if (this.getThisPart() != fp0.getThisPart()) return false;
    if (!this.getPrimaryIP().equals(fp0.getPrimaryIP())) return false;
    if (!this.getSecondaryIP().equals(fp0.getSecondaryIP())) return false;
    return true;
  }
  /**
   * Create an array of Spark partition objects for HPCC file parts.
   * @param num_parts the number of parts for the file
   * @param dir the directory name for the file
   * @param name the base name of the file
   * @param mask the mask for the file name file part suffix
   * @param parts an array of JAPI file part info objects
   * @param cr an address re-mapper for THOR clusters on virtual networks
   * @return an array of partitions for Spark
   */
  public static FilePart[] makeFileParts(int num_parts, String dir,
      String name, String mask, DFUFilePartInfo[] parts,
      ClusterRemapper cr, boolean compressed_flag) throws HpccFileException {
    FilePart[] rslt = new FilePart[num_parts];
    Arrays.sort(parts, FilePartInfoComparator);
    int copies = parts.length / num_parts;
    int posSecondary = (copies==1) ? 0 : 1;
    for (int i=0; i<num_parts; i++) {
      DFUFilePartInfo primary = parts[i * copies];
      DFUFilePartInfo secondary = parts[(i * copies) + posSecondary];
      int partSize;
      try {
        partSize = (primary.getPartsize()!="")
            ? fmt.parse(primary.getPartsize()).intValue()  : 0;
      } catch (ParseException e) {
        partSize = 0;
      }
      rslt[i] = new FilePart(cr.revisePrimaryIP(primary),
          cr.reviseSecondaryIP(secondary),
          dir, name, i+1, num_parts, partSize, mask,
          cr.reviseClearPort(primary), cr.reviseSslPort(primary),
          compressed_flag);
    }
    return rslt;
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
