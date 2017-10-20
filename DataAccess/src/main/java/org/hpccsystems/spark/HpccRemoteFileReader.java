package org.hpccsystems.spark;

import org.hpccsystems.spark.HpccFileException;
import org.hpccsystems.spark.thor.BinaryRecordReader;


/**
 * @author holtjd
 * Remote file reader used by the HpccRDD.
 */
public class HpccRemoteFileReader {
  private RecordDef def;
  private FilePart fp;
  private BinaryRecordReader brr;
  /**
   * A remote file reader that reads the part identified by the
   * FilePart object using the record definition provided.
   * @param def the definition of the data
   * @param fp the part of the file, name and location
   */
  public HpccRemoteFileReader(FilePart fp, RecordDef rd) {
    this.def = rd;
    this.fp = fp;
    this.brr = new BinaryRecordReader(fp, def);
  }
  /**
   * Is there more data
   * @return true if there is a next record
   */
  public boolean hasNext() {
    boolean rslt;
    try {
      rslt = brr.hasNext();
    } catch (HpccFileException e) {
      rslt = false;
      System.err.println("Read failure for " + fp.toString());
      e.printStackTrace(System.err);
    }
    return rslt;
  }
  /**
   * Return next record
   * @return the record
   */
  public Record next() {
    Record rslt = null;
    try {
      rslt = brr.getNext();
    } catch (HpccFileException e) {
      System.err.println("Read failure for " + fp.toString());
      e.printStackTrace(System.err);
      throw new java.util.NoSuchElementException("Fatal read error");
    }
    return rslt;
  }
}
