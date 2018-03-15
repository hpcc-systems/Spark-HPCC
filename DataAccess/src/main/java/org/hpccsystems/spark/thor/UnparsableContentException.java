package org.hpccsystems.spark.thor;

/**
 * @author holtjd
 * The data block from the HPCC Cluster held unexpected values
 * and could not be parsed successfully.
 */
public class UnparsableContentException extends Exception {
  private static final long serialVersionUID = 1l;
  /**
   * Empty constructor
   */
  public UnparsableContentException() {
  }

  /**
   * @param message
   */
  public UnparsableContentException(String message) {
    super(message);
  }

  /**
   * @param cause
   */
  public UnparsableContentException(Throwable cause) {
    super(cause);
  }

  /**
   * @param message
   * @param cause
   */
  public UnparsableContentException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * @param message
   * @param cause
   * @param enableSuppression
   * @param writableStackTrace
   */
  public UnparsableContentException(String message, Throwable cause, boolean enableSuppression,
      boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
