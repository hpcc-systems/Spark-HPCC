package org.hpccsystems.spark;

import java.io.Serializable;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;
import org.hpccsystems.spark.thor.FieldDef;

public class IntegerContent extends Content implements Serializable {
  private final static long serialVersionUID = 1L;
  private long value;
  /**
   * Constructor for serialization
   */
  protected IntegerContent() {
    super();
    this.value = 0;
  }
  /**
   * Convenience constructor when no field def is available
   * @param name
   * @param value
   */
  public IntegerContent(String name, long v) {
    super(FieldType.INTEGER, name);
    this.value = v;
  }
  /**
   * Normal constructor
   * @param def the field definition
   * @param v the value
   */
  public IntegerContent(FieldDef def, long v) {
    super(def);
    if (def.getFieldType() != FieldType.INTEGER) {
      throw new IllegalArgumentException("Def must have Integer type");
    }
    this.value = v;
  }
  /**
   * The content in the raw format
   * @return the value
   */
  public long asInt() {
    return value;
  }

  @Override
  public int numFields() {
    return 1;
  }

  @Override
  public String asString(String fieldSep, String elementSep) {
    String rslt = Long.toString(this.value);
    return rslt;
  }

  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = Long.toString(this.value);
    return rslt;
  }
  @Override
  public Object asRowObject(DataType dtyp) {
    if (!DataTypes.LongType.sameType(dtyp)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expect double type, given ");
      sb.append(dtyp.typeName());
      throw new IllegalArgumentException(sb.toString());
    }
    return new Long(this.value);
  }

}
