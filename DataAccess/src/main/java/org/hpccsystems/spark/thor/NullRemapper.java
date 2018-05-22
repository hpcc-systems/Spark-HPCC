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
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;

/**
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
