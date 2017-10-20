/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * @author holtjd
 * Exception class for problems accessing files on an HPCC Cluster.
 */
public class HpccFileException extends Exception implements Serializable {
  static private final long serialVersionUID = 1L;
  /**
   * Empty constructor.
   */
  public HpccFileException() {
  }

  /**
   * @param message text explaining exception
   */
  public HpccFileException(String message) {
    super(message);
  }

  /**
   * @param cause An exception that has been remapped into an HPCC Exception
   */
  public HpccFileException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message explanation of exception
   * @param cause generating exception
   */
  public HpccFileException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * @param message
   * @param cause
   * @param enableSuppression
   * @param writableStackTrace
   */
  public HpccFileException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
    // TODO Auto-generated constructor stub
  }

}
