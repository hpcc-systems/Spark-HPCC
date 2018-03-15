package org.hpccsystems.spark.thor;

import org.hpccsystems.spark.HpccFileException;
import org.hpccsystems.spark.Record;

/**
 * @author holtjd
 * Interface for the HPCC Systems remote file readers.
 */
public interface IRecordReader {
  /**
   * Are there more records?  The first time used will trigger a
   * remote file read.
   * @return true if there is at least one more record
   * @throws HpccFileException if there was a failure on the back end.  This
   * error is not recoverable by a retry.
   */
  public boolean hasNext() throws HpccFileException;
  /**
   * Produce the next record.
   * @return a record
   * @throws HpccFileException error on the back end, not recoverable
   */
  public Record getNext() throws HpccFileException;
}
