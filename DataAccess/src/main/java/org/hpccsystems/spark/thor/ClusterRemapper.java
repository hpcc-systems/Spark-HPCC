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

import org.hpccsystems.spark.HpccFileException;
import org.hpccsystems.ws.client.gen.wsdfu.v1_39.DFUPartLocation;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;
import org.hpccsystems.ws.client.wrappers.wsdfu.DFUFileCopyWrapper;

/**
 * Re-map address information for Clusters that can
 * only be reached through alias addresses.
 * Addresses are re-mapped to a range of IP addresses or to an
 * IP address and a range of ports.
 */
public abstract class ClusterRemapper {
  protected int nodes;
  protected final static int DEFAULT_CLEAR = 7100;
  protected final static int DEFAULT_SSL = 7700;

  /**
   * Constructor for common information.
   */
  protected ClusterRemapper(RemapInfo ri) throws HpccFileException{
    this.nodes = ri.getNodes();
  }

  /**
   * The optionally revised array of hosts.
   * @param hosts information
   * @return Revised IP address as strings
   */
  public abstract String[] reviseIPs(String[] hosts)
		  throws HpccFileException;

  /**
   * The optionally revised array of file part copy IPs.
   * @param fpi file part information
   * @return an IP address as a string
   */
  public abstract String [] reviseIPs(DFUFileCopyWrapper[] dfuFileCopies)
          throws HpccFileException;
  /**
   * The clear communications port number or zero if clear communication
   * is not accepted
   * @param fpi file part information
   * @return the port number
   */
  public abstract int reviseClearPort(DFUFilePartInfo fpi);
  /**
   * The SSL communications port number of zero if SSL is not supported
   * @param fpi the file part information
   * @return the port number
   */
  public abstract int reviseSslPort(DFUFilePartInfo fpi);
  /**
   * Factory for making a cluster re-map.
   * @param ri the re-mapping information
   * @param strings a list of file part locations
   * @return a re-mapping object consistent with the provided information
   * @throws HpccFileException
   */

  public static ClusterRemapper makeMapper(RemapInfo ri, String[] strings) throws HpccFileException
  {
    ClusterRemapper rslt = (ri.isNullMapper()) ? new NullRemapper(ri) : (ri.isPortAliasing()) ? new PortRemapper(ri) : new AddrRemapper(ri, strings);
    return rslt;
  }
}
