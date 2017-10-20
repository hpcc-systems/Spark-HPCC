package org.hpccsystems.spark;

import java.io.Serializable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hpccsystems.spark.thor.FieldDef;

public class RealContent extends Content implements Serializable {
  private final static long serialVersionUID = 1L;
  private double value;
  /**
   * Empty constructor for serialization.
   */
  protected RealContent() {
    this.value = 0;
  }
  /**
   * Convenience constructor when FieldDef is not available
   * @param name the field name
   * @param v the value of the content
   */
  public RealContent(String name, double v) {
    super(FieldType.REAL, name);
    this.value = v;
  }
  /**
   * Normal constructor
   * @param def
   * @param v
   */
  public RealContent(FieldDef def, double v) {
    super(def);
    if (def.getFieldType()!=FieldType.REAL) {
      throw new IllegalArgumentException("Field definition has wrong type");
    }
    this.value = v;
  }
  /**
   * The content in raw form.
   * @return
   */
  public double asReal() {
    return this.value;
  }

  @Override
  public int numFields() {
    return 1;
  }

  @Override
  public String asString(String fieldSep, String elementSep) {
    String rslt = Double.toString(this.value);
    return rslt;
  }
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = Double.toString(this.value);
    return rslt;
  }
  @Override
  public Object asRowObject(DataType dtyp) {
    if (!DataTypes.DoubleType.sameType(dtyp)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected type Double, given ");
      sb.append(dtyp.typeName());
      throw new IllegalArgumentException(sb.toString());
    }
    return new Double(this.value);
  }

}
