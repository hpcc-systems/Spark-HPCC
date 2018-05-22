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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.hpccsystems.spark.HpccFileException;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;

/**
 * Map internal IP addresses to external IP addresses.  Note that the
 * relation of parts to IP address must be in lexical order.
 *
 */
public class AddrRemapper extends ClusterRemapper {
  private HashMap<String, String> ip_tab;

  public AddrRemapper(RemapInfo ri, DFUFilePartInfo[] fpiList, int parts)
      throws HpccFileException {
    super(ri);
    if (!ri.isIpAliasing()) {
      throw new IllegalArgumentException("Inappropriate type of re-mapping info");
    }
    int primaries = 0;
    for (DFUFilePartInfo fpi : fpiList) {
      if (fpi.getCopy() == 1) primaries++;
    }
    if (primaries != parts) {
      throw new HpccFileException("Number of primary != number of parts");
    }
    int pos = 0;
    DFUFilePartInfo[] primary_list = new DFUFilePartInfo[primaries];
    for (DFUFilePartInfo fpi : fpiList) {
      if (fpi.getCopy()==1) primary_list[pos++] = fpi;
    }
    Arrays.sort(primary_list, FilePartInfoComparator);  //copy then part
    short[][] ip_list_parts = new short[ri.getNodes()][];
    ip_list_parts[0] = new short[4];
    StringTokenizer st = new StringTokenizer(primary_list[0].getIp(), ".");
    if (st.countTokens()!=4) {
      throw new HpccFileException("Incomplete IP addresses for parts");
    }
    pos = 0;
    while (st.hasMoreTokens()) {
      ip_list_parts[0][pos] = Short.parseShort(st.nextToken());
      pos++;
    }
    for (int i=1; i<ri.getNodes(); i++) {
      st = new StringTokenizer(primary_list[i].getIp(), ".");
      if (st.countTokens() != 4) {
        throw new HpccFileException("Incomplete IP addresses for parts");
      }
      pos = 0;
      ip_list_parts[i] = new short[4];
      while (st.hasMoreTokens()) {
        ip_list_parts[i][pos] = Short.parseShort(st.nextToken());
        pos++;
      }
      if (ip_list_parts[i][0] >= ip_list_parts[i-1][0]
       && ip_list_parts[i][1] >= ip_list_parts[i-1][1]
       && ip_list_parts[i][2] >= ip_list_parts[i-1][2]
       && ip_list_parts[i][3] >  ip_list_parts[i-1][3]) continue;
      throw new HpccFileException("Bad IP to part number relation");
    }
    short[] target_parts = new short[4];
    st = new StringTokenizer(ri.getBaseIp(), ".");
    if (st.countTokens() != 4) {
      throw new IllegalArgumentException("Incomplete IP address for target");
    }
    pos = 0;
    while (st.hasMoreTokens()) {
      target_parts[pos] = Short.parseShort(st.nextToken());
      pos++;
    }
    // networks are no longer class based, no overflow not checked
    ip_tab = new HashMap<String, String>(ri.getNodes()*2);
    for (int i=0; i<ri.getNodes(); i++) {
      StringBuilder sb = new StringBuilder();
      for (int j=0; j<4; j++) {
        sb.append(target_parts[j]);
        if (j<3) sb.append(".");
      }
      ip_tab.put(primary_list[i].getIp(), sb.toString());
      target_parts[3]++;
      if (target_parts[3] < 256) continue;
      target_parts[3] = 0;
      target_parts[2]++;
      if (target_parts[2] < 256) continue;
      target_parts[2] = 0;
      target_parts[1]++;
      if (target_parts[1] < 256) continue;
      throw new IllegalArgumentException("Too many nodes for starting address");
    }
  }

  @Override
  public String revisePrimaryIP(DFUFilePartInfo fpi) throws HpccFileException {
    if (!this.ip_tab.containsKey(fpi.getIp())) {
      throw new HpccFileException("IP not in table");
    }
    return ip_tab.get(fpi.getIp());
  }

  @Override
  public String reviseSecondaryIP(DFUFilePartInfo fpi) throws HpccFileException {
    if (!this.ip_tab.containsKey(fpi.getIp())) {
      throw new HpccFileException("IP not in table");
    }
    return ip_tab.get(fpi.getIp());
  }

  @Override
  public int reviseClearPort(DFUFilePartInfo fpi) {
    return DEFAULT_CLEAR;
  }

  @Override
  public int reviseSslPort(DFUFilePartInfo fpi) {
    return DEFAULT_SSL;
  }
  /**
   * Comparator to re-order the file parts.
   */
  private static Comparator<DFUFilePartInfo> FilePartInfoComparator
  = new Comparator<DFUFilePartInfo>() {
        public int compare(DFUFilePartInfo fpi1, DFUFilePartInfo fpi2) {
          if (fpi1.getCopy() < fpi2.getCopy()) return -1;
          if (fpi1.getCopy() > fpi2.getCopy()) return 1;
          if (fpi1.getId() < fpi2.getId()) return -1;
          if (fpi1.getId() > fpi2.getId()) return 1;
          return 0;
        }
};

}
