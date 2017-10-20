package org.hpccsystems.spark;

import java.io.Serializable;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;
import org.hpccsystems.spark.thor.FieldDef;

/**
 * Contains a Boolean value.
 * @author holtjd
 *
 */
public class BooleanContent extends Content implements Serializable {
private static final long serialVersionUID = 1l;
private boolean value;
  /**
   * Empty constructor for serialization support.
   */
  protected BooleanContent() {
    this.value = false;
  }

  /**
   * @param name the field name for this content
   * @param v the value
   */
  public BooleanContent(String name, boolean v) {
    super(FieldType.BOOLEAN, name);
    this.value = v;
  }

  /**
   * @param def the field definition of this content
   * @parm v the value
   */
  public BooleanContent(FieldDef def, boolean v) {
    super(def);
    if (def.getFieldType() != FieldType.BOOLEAN){
      throw new IllegalArgumentException("Field definition has wrong type");
    }
    this.value = v;
  }
  /**
   * The content of this item in raw form
   * @return true or false
   */
  public boolean asBool() {
    return this.value;
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
    String rslt = Boolean.toString(this.value);
    return rslt;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = Boolean.toString(this.value);
    return rslt;
  }

  @Override
  public Object asRowObject(DataType dtyp) {
    if (!DataTypes.BooleanType.sameType(dtyp)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected boolean, given ");
      sb.append(dtyp.typeName());
      throw new IllegalArgumentException(sb.toString());
    }
    return new Boolean(this.value);
  }

}
