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

/**
 * Information to re-map address information for Clusters that can
 * only be reached through alias addresses.
 * Addresses are re-mapped to a range of IP addresses or to an
 * IP address and a range of ports.
 */
public class RemapInfo implements Serializable {
  static private final long serialVersionUID = 1L;
  private final int nodes;
  private final String base_ip;
  private final int base_portClear;
  private final int base_portSsl;
  /**
   * Info to create a null re-map.
   * @param thorNodes number of THOR nodes in this cluster
   */
  public RemapInfo(int thorNodes) {
    this.nodes = thorNodes;
    this.base_portClear = 0;
    this.base_portSsl = 0;
    this.base_ip = "";
  }
  /**
   * Info to create a re-mapping to a range of IP addresses
   * @param thorNodes number of noder for this THOR cluster
   * @param ip first alias IP
   */
  public RemapInfo(int thorNodes, String ip) {
    this.nodes = thorNodes;
    this.base_portClear = 0;
    this.base_portSsl = 0;
    this.base_ip = ip;
  }
  /**
   * Info to create a re-mapping to a single IP and a range of ports.  The
   * port number should be zero for the case (clear or SSL) that is not
   * supported by the cluster.  Both can be supported.  If both ports are
   * zero, the re-map will be to a range of IP addresses
   * @param thorNodes number of nodes for this THOR cluster
   * @param ip the IP for the cluster
   * @param portClear the first port in the range for clear exchange
   * @param portSsl the first port in the range for SSL exchange
   */
  public RemapInfo(int thorNodes, String ip, int portClear, int portSsl) {
    this.nodes = thorNodes;
    this.base_portClear = portClear;
    this.base_portSsl = portSsl;
    this.base_ip = ip;
  }
  /**
   * The number of nodes in the THOR cluster.
   * @return number of nodes
   */
  public int getNodes() { return this.nodes; }
  /**
   * Base port number for clear exchange with the cluster or zero if not used
   * @return clear port number
   */
  public int getBasePortClear() { return this.base_portClear; }
  /**
   * Base port number for SSL exchange with the cluster or zero if not re-mapped
   * @return SSL port number
   */
  public int getBasePortSsl() { return this.base_portSsl; }
  /**
   * Get base IP for range or IP for port range
   * @return IP
   */
  public String getBaseIp() { return this.base_ip; }
  /**
   * Is this a re-map to a sequence of IP alias values
   * @return
   */
  public boolean isIpAliasing() {
    return (!this.base_ip.equals("")) && this.base_portSsl==0
        && this.base_portSsl==0;
  }
  /**
   * Is this a null re-mapper
   * @return
   */
  public boolean isNullMapper() { return this.base_ip.equals(""); }
  /**
   * Is this to re-map a THOR cluster to a single IP and a sequence of ports?
   * @return
   */
  public boolean isPortAliasing() {
    return this.base_portClear != 0 || this.base_portSsl != 0;
  }
}
