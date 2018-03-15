package org.hpccsystems.spark.thor;

import org.hpccsystems.ws.client.platform.DFUFilePartInfo;
import java.util.Comparator;
import org.hpccsystems.spark.HpccFileException;

/**
 * @author holtjd
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
   * The optionally revised primary IP for this part.
   * @param fpi file part information
   * @return an IP address as a string
   */
  public abstract String revisePrimaryIP(DFUFilePartInfo fpi)
                        throws HpccFileException;
  /**
   * Thie optionally revised secondary IP or blank if no copy
   * @param fpi file part information
   * @return an IP address or blank string if no copy is available
   */
  public abstract String reviseSecondaryIP(DFUFilePartInfo fpi)
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
   * @param fpiList a list of the file parts
   * @param parts the number of file parts
   * @return a re-mapping object consistent with the provided information
   * @throws HpccFileException
   */
  public static ClusterRemapper makeMapper(RemapInfo ri,
      DFUFilePartInfo[] fpiList, int parts) throws HpccFileException {
    ClusterRemapper rslt = (ri.isNullMapper()) ? new NullRemapper(ri)
        : (ri.isPortAliasing()) ? new PortRemapper(ri)
            : new AddrRemapper(ri, fpiList, parts);
    return rslt;
  }
}
