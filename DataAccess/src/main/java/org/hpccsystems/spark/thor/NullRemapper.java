package org.hpccsystems.spark.thor;

import org.hpccsystems.ws.client.platform.DFUFilePartInfo;
import org.hpccsystems.spark.HpccFileException;

/**
 * @author holtjd
 * A no action re-map of the address.  Does provide the port information.
 */
public class NullRemapper extends ClusterRemapper {

  /**
   * @param ri
   */
  public NullRemapper(RemapInfo ri) throws HpccFileException {
    super(ri);
    if (!ri.isNullMapper()) {
      throw new IllegalArgumentException("Incompatible re-mapping information");
    }
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.thor.ClusterRemapper#revisePrimaryIP(org.hpccsystems.ws.client.platform.DFUFilePartInfo)
   */
  @Override
  public String revisePrimaryIP(DFUFilePartInfo fpi) throws HpccFileException {
    if (fpi.getCopy() != 1) {
      throw new IllegalArgumentException("Secondary part info provided");
    }
    return fpi.getIp();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.thor.ClusterRemapper#reviseSecondaryIP(org.hpccsystems.ws.client.platform.DFUFilePartInfo)
   */
  @Override
  public String reviseSecondaryIP(DFUFilePartInfo fpi) throws HpccFileException {
    return fpi.getIp();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.thor.ClusterRemapper#reviseClearPort(org.hpccsystems.ws.client.platform.DFUFilePartInfo)
   */
  @Override
  public int reviseClearPort(DFUFilePartInfo fpi) {
    return DEFAULT_CLEAR;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.thor.ClusterRemapper#reviseSslPort(org.hpccsystems.ws.client.platform.DFUFilePartInfo)
   */
  @Override
  public int reviseSslPort(DFUFilePartInfo fpi) {
    return DEFAULT_SSL;
  }

}
