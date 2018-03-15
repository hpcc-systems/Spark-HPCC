package org.hpccsystems.spark;

import java.io.Serializable;
import javax.xml.bind.DatatypeConverter;
import org.hpccsystems.spark.thor.FieldDef;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.BinaryType;

/**
 * Binary field content.  Either a DATA field or a DATAn fixed length field.
 * @author holtjd
 *
 */
public class BinaryContent extends Content implements Serializable {
  static private final long serialVersionUID = 1l;
  private byte[] value;
  /**
   * Empty constructor for serialization
   */
  protected BinaryContent() {
    this.value = new byte[0];
  }

  /**
   * @param name
   * @param v byte array value
   */
  public BinaryContent(String name, byte[] v) {
    super(FieldType.BINARY, name);
    this.value = new byte[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
  }

  /**
   * @param def
   */
  public BinaryContent(FieldDef def, byte[] v) {
    super(def);
    if (def.getFieldType() != FieldType.BINARY){
      throw new IllegalArgumentException("Field definition has wrong type");
    }
    this.value = new byte[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
  }
  /**
   * The binary content as a byte array
   * @return
   */
  public byte[] asBinary() {
    byte[] rslt = new byte[this.value.length];
    for (int i=0; i<this.value.length; i++) rslt[i] = this.value[i];
    return rslt;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#numFields()
   */
  @Override
  public int numFields() {
    return 1;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asString()
   */
  @Override
  public String asString(String fieldSep, String elementSep) {
    String rslt = DatatypeConverter.printHexBinary(this.value);
    return rslt;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = this.asString();
    return rslt;
  }

  @Override
  public Object asRowObject(DataType dtyp) {
    if (!dtyp.sameType(DataTypes.BinaryType)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected BinaryType, given ");
      sb.append(dtyp.toString());
      throw new IllegalArgumentException(sb.toString());
    }
    byte[] rslt = new byte[this.value.length];
    for (int i=0; i<this.value.length; i++) rslt[i] = this.value[i];
    return rslt;
  }

}
