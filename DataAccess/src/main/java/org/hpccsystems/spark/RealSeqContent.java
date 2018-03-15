package org.hpccsystems.spark;

import java.io.Serializable;
import java.util.ArrayList;
import scala.collection.JavaConverters;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hpccsystems.spark.thor.FieldDef;

/**
 * @author holtjd
 * A set or array of real values.
 */
public class RealSeqContent extends Content implements Serializable {
  private static final long serialVersionUID = 1L;
  private boolean isAll;
  private double[] value;

  /**
   * Empty constructor for serialization
   */
  protected RealSeqContent() {
    this.value = new double[0];
    this.isAll = false;
  }

  /**
   * @param name the field name
   * @param v the value for this content item
   * @param f Universal set, all values
   */
  public RealSeqContent(String name, double[] v, boolean f) {
    super(FieldType.SET_OF_REAL, name);
    this.value = new double[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
    this.isAll = f;
  }

  /**
   * @param def
   * @param v the set of values
   * @param f Universal set, all values
   */
  public RealSeqContent(FieldDef def, double[] v, boolean f) {
    super(def);
    if (def.getFieldType() != FieldType.SET_OF_REAL) {
      throw new IllegalArgumentException("Incorrect type for field definition");
    }
    this.value = new double[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
    this.isAll = f;
  }
  /**
   * The content value in raw form
   * @return
   */
  public double[] asSetofReal() {
    double[] rslt = new double[this.value.length];
    for (int i=0; i<this.value.length; i++) rslt[i] = this.value[i];
    return rslt;
  }
  /**
   * Is this the universe of values
   * @return
   */
  public boolean isAllValues() { return isAll;  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#numFields()
   */
  @Override
  public int numFields() {
    return 1;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asString(java.lang.String, java.lang.String)
   */
  @Override
  public String asString(String fieldSep, String elementSep) {
    StringBuilder sb = new StringBuilder(20 + this.value.length*10);
    for (int i=0; i<this.value.length; i++) {
      if (i>0) sb.append(elementSep);
      sb.append(Double.toString(this.value[i]));
    }
    return sb.toString();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[this.value.length];
    for (int i=0; i<this.value.length; i++) {
        rslt[i] = Double.toString(this.value[i]);
    }
    return rslt;
  }

  @Override
  public Object asRowObject(DataType dtyp) {
    DataType test = DataTypes.createArrayType(DataTypes.DoubleType);
    if (!test.sameType(dtyp)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expect array of double, given ");
      sb.append(dtyp.typeName());
      throw new IllegalArgumentException(sb.toString());
    }
    ArrayList<Double> work = new ArrayList<Double>(this.value.length);
    for (int i=0; i<this.value.length; i++) {
      work.add(new Double(this.value[i]));
    }
    return JavaConverters.asScalaBufferConverter(work).asScala().seq();
  }

}
