package org.hpccsystems.spark.thor;

/**
 * @author holtjd
 * This exception is thrown with the data definition cannot be
 * used to retrieve or describe the data.  The caller will need
 * to use a different file.
 */
public class UnusableDataDefinitionException extends Exception {
  /**
   * @param message a message explaining the condition
   */
  public UnusableDataDefinitionException(String message) {
    super(message);
  }
  /**
   * @param cause
   */
  /**
   * @param message a message explaining the condition
   * @param cause an exception that motivated this exception
   */
  public UnusableDataDefinitionException(String message, Throwable cause) {
    super(message, cause);
  }

}
