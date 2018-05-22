/*******************************************************************************
 *     HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *******************************************************************************/
package org.hpccsystems.spark;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hpccsystems.spark.thor.FieldDef;

import scala.collection.JavaConverters;

/**
 * Content for SET OF INTEGER or SET OF UNSIGNED
 */
public class IntegerSeqContent extends Content implements Serializable {
  private static final long serialVersionUID = 1L;
  private boolean isAll;
  private long[] value;
  /**
   * Empty constructor for serialization support
   */
  public IntegerSeqContent() {
    this.value = new long[0];
    this.isAll = false;
  }

  /**
   * @param name
   * @param v content values
   * @param f Universal set, all values
   */
  public IntegerSeqContent(String name, long[] v, boolean f) {
    super(FieldType.SET_OF_INTEGER, name);
    this.value = new long[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
    this.isAll = f;
  }

  /**
   * @param def
   * @param v content values
   * @param f Universal set, all values
   */
  public IntegerSeqContent(FieldDef def, long[] v, boolean f) {
    super(def);
    if (def.getFieldType() != FieldType.SET_OF_INTEGER) {
      throw new IllegalArgumentException("Incorrect field type");
    }
    this.value = new long[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
    this.isAll = f;
  }
  /**
   * Content in raw form as a set of long integers
   * @return the content
   */
  public long[] asSetOfInt() {
    long[] rslt = new long[this.value.length];
    for (int i=0; i<this.value.length; i++) rslt[i] = this.value[i];
    return rslt;
  }
  /**
   * Is this the universe of values
   * @return
   */
  public boolean isAllValues() { return this.isAll; }

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
    StringBuilder sb = new StringBuilder(10 + this.value.length*10);
    for (int i=0; i<this.value.length; i++) {
      if (i>0) sb.append(elementSep);
      sb.append(Long.toString(this.value[i]));
    }
    return sb.toString();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[this.value.length];
    for (int i=0; i<this.value.length; i++) rslt[i] = Long.toString(this.value[i]);
    return rslt;
  }

  @Override
  public Object asRowObject(DataType dtyp) {
    DataType test = DataTypes.createArrayType(DataTypes.LongType);
    if (!test.sameType(dtyp)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expect array of long type, given ");
      sb.append(dtyp.typeName());
      throw new IllegalArgumentException(sb.toString());
    }
    ArrayList<Long> work = new ArrayList<Long>(this.value.length);
    for (int i=0; i<this.value.length; i++) {
      work.add(new Long(this.value[i]));
    }
    return JavaConverters.asScalaBufferConverter(work).asScala().seq();
  }

}
