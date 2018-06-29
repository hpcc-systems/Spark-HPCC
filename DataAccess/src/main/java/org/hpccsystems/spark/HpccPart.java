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
import org.hpccsystems.spark.thor.DataPartition;
import org.hpccsystems.spark.thor.FileFilter;
import org.hpccsystems.spark.thor.RemapInfo;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;

/**
 * A file part of an HPCC file.  This is the Spark partition for the RDD.
 *
 */
public class HpccPart implements Partition, Serializable {
  static private final long serialVersionUID = 1L;
  private DataPartition dataPart;
  private int this_part;
  private int num_parts;

  /**
   * Construct the file part, used by makeParts
   * @param parts the number of partitions
   * @param part_ordinal the ordinal position of this part
   * @param part the data partition
   */
  private HpccPart(int parts, int part_ordinal, DataPartition part) {
    this.dataPart = part;
    this.this_part = part_ordinal;
    this.num_parts = parts;
  }

  /**
   * Partition information
   * @return information
   */
  public DataPartition getPartitionInfo() { return this.dataPart; }
  /* (non-Javadoc)
   * @see org.apache.spark.Partition#index()
   */
  public int index() {
    return this.this_part - 1;
  }
  /*
   * (non-Javadoc)
   * @see java.lang.Object
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(this.this_part);
    sb.append(" ");
    sb.append(this.dataPart.toString());
    return sb.toString();
  }
  /* (non-Javadoc)
   * Spark core 2.10 needs this defined, not needed in 2.11
   */
  public boolean org$apache$spark$Partition$$super$equals(Object arg0) {
    if (!(arg0 instanceof HpccPart)) return false;
    HpccPart fp0 = (HpccPart) arg0;
    DataPartition dp0 = fp0.getPartitionInfo();
    if (!this.dataPart.equals(dp0)) return false;
    if (this.this_part != fp0.this_part) return false;
    if (this.num_parts != fp0.num_parts) return false;
    return true;
  }
  /**
   * Create an array of Spark partition objects for HPCC file parts.
   * @param fdi File detail information for the file
   * @param max_parts the maximum number of partitions or zero for no max
   * @param filter a filter expression to select records which may be
   * set to FileFilter.nullFilter() for all records.
   * @return an array of partitions for Spark
   */
  public static HpccPart[] makeFileParts(DFUFileDetailInfo fdi,
      RemapInfo remap_info, int max_parts, FileFilter filter)
          throws HpccFileException {
    DataPartition[] dataParts
              = DataPartition.createPartitions(fdi, remap_info, max_parts, filter);
    HpccPart[] rslt = new HpccPart[dataParts.length];
    for (int i=0; i<rslt.length; i++ ) {
      rslt[i] = new HpccPart(dataParts.length, i+1, dataParts[i]);
    }
    return rslt;
  }
}
