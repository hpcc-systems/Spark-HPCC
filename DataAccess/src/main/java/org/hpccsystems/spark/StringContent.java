package org.hpccsystems.spark;

import java.io.Serializable;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;
import org.hpccsystems.spark.thor.FieldDef;

/**
 * A field content item with String as the native type.
 * @author holtjd
 *
 */
public class StringContent extends Content implements Serializable {
  private static final long serialVersionUID = 1L;
  private String value;
  /**
   * No argument constructor for serialization
   */
  protected StringContent() {
    super();
    this.value = "";
  }
  /**
   * Construct a StringContent item without a FieldDef
   * @param v the string value
   * @param name the name of the field
   */
  public StringContent(String name, String v) {
    super(FieldType.STRING, name);
    this.value = v;
  }
  /**
   * Construct a StringContent item in the normal manner
   * @param def the FieldDef for this field
   * @param v the value of the field
   */
  public StringContent(FieldDef def, String v) {
    super(def);
    if (def.getFieldType() != FieldType.STRING) {
      throw new IllegalArgumentException("def must be String type");
    }
    this.value = v;
  }

  /*
   * (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#numFields()
   */
  @Override
  public int numFields() { return 1; }
  /* (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asInt()
   */
  @Override
  public String asString(String fieldSep, String elementSep) {
    return this.value;
  }
  /* (non-javadoc)
   * @see org.hpccsystems.spark.FieldContent#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = this.asString();
    return rslt;
  }
  @Override
  public Object asRowObject(DataType dtyp) {
    if (!DataTypes.StringType.sameType(dtyp)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected String type, given ");
      sb.append(dtyp.typeName());
      throw new IllegalArgumentException(sb.toString());
    }
    return this.value;
  }

}
