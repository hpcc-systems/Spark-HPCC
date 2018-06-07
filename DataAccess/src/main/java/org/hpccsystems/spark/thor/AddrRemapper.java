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
import java.util.HashSet;
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
  /**
   * Remapping of the IP addresses for virtual clusters.  Note that there
   * can be more parts than nodes or fewer parts than nodes, though usually
   * the number of parts equals the number of nodes for files and is one
   * higher for keys.
   * @param ri re-mapping information for an address re-mapper
   * @param fpiList file part list for the file is used as the source of IP values
   * that need to be mapped
   * @throws HpccFileException when something is wrong with the info
   */
  public AddrRemapper(RemapInfo ri, DFUFilePartInfo[] fpiList)
      throws HpccFileException {
    super(ri);
    if (!ri.isIpAliasing()) {
      throw new IllegalArgumentException("Inappropriate type of re-mapping info");
    }
    HashSet<String> ip_set = new HashSet<String>(fpiList.length * 2);
    for (DFUFilePartInfo fp : fpiList) {
      ip_set.add(fp.getIp());
    }
    String[] ip_list = ip_set.toArray(new String[0]);
    Arrays.sort(ip_list);
    if (ip_list.length > ri.getNodes()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Too many addresses, need ");
      sb.append(ip_list.length);
      sb.append(" but have only ");
      sb.append(ri.getNodes());
      throw new HpccFileException(sb.toString());
    }
    short[] target_parts = new short[4];
    StringTokenizer st = new StringTokenizer(ri.getBaseIp(), ".");
    if (st.countTokens() != 4) {
      throw new IllegalArgumentException("Incomplete IP address for target");
    }
    int pos = 0;
    while (st.hasMoreTokens()) {
      target_parts[pos] = Short.parseShort(st.nextToken());
      pos++;
    }
    ip_tab = new HashMap<String, String>(ip_list.length*2);
    for (int i=0; i<ip_list.length; i++) {
      StringBuilder sb = new StringBuilder();
      for (int j=0; j<4; j++) {
        sb.append(target_parts[j]);
        if (j<3) sb.append(".");
      }
      ip_tab.put(ip_list[i], sb.toString());
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
