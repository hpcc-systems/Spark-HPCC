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
package org.hpccsystems.spark.thor;

import org.hpccsystems.spark.HpccFileException;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;
import org.hpccsystems.ws.client.wrappers.wsdfu.DFUFileCopyWrapper;

/**
 * Re-maps addresses to a single IP and a range of ports.  Used when the THOR
 * cluster is on a virtual network and ports on a host in this network have
 * been aliased to the addresses on the virtual network.
 * Note that this approach eliminates the alternate copy.  The File Part object
 * expects that the alternate is reached via a different IP so there is no
 * alternative port available.
 */
public class PortRemapper extends ClusterRemapper {
  private int portClear;
  private int portSsl;
  private String base_ip;
  /**
   * @param ri
   */
  public PortRemapper(RemapInfo ri) throws HpccFileException {
    super(ri);
    if (!ri.isPortAliasing()) {
      throw new IllegalArgumentException("Incompatible re-mapping information");
    }
    this.base_ip = ri.getBaseIp();
    this.portClear = ri.getBasePortClear();
    this.portSsl = ri.getBasePortSsl();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.thor.ClusterRemapper#reviseClearPort(org.hpccsystems.ws.client.platform.DFUFilePartInfo)
   */
  @Override
  public int reviseClearPort(DFUFilePartInfo fpi) {
    return (this.portClear==0)  ? 0 : this.portClear-1+fpi.getId();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.thor.ClusterRemapper#reviseSslPort(org.hpccsystems.ws.client.platform.DFUFilePartInfo)
   */
  @Override
  public int reviseSslPort(DFUFilePartInfo fpi) {
    return (this.portSsl==0)  ? 0  : this.portSsl-1+fpi.getId();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.thor.ClusterRemapper#reviseIPs(org.hpccsystems.ws.client.platform.DFUFilePartInfo[])
   */
  @Override
  public String[] reviseIPs(DFUFileCopyWrapper[] dfuFileCopies) throws HpccFileException
  {
     return new String[] {base_ip};
  }

  /* (non-Javadoc)
  * @see org.hpccsystems.spark.thor.ClusterRemapper#reviseIPs(java.lang.String[])
  */
  @Override
  public String[] reviseIPs(String[] hosts) throws HpccFileException {
    return new String[] {base_ip};
  }

}
