/**
 *
 */
package org.hpccsystems.spark.thor;

import org.hpccsystems.ws.client.platform.DFUFilePartInfo;
import org.hpccsystems.spark.HpccFileException;

/**
 * @author holtjd
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
   * @see org.hpccsystems.spark.thor.ClusterRemapper#revisePrimaryIP(org.hpccsystems.ws.client.platform.DFUFilePartInfo)
   */
  @Override
  public String revisePrimaryIP(DFUFilePartInfo fpi) throws HpccFileException {
    return this.base_ip;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.thor.ClusterRemapper#reviseSecondaryIP(org.hpccsystems.ws.client.platform.DFUFilePartInfo)
   */
  @Override
  public String reviseSecondaryIP(DFUFilePartInfo fpi) throws HpccFileException {
    return "";
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

}
