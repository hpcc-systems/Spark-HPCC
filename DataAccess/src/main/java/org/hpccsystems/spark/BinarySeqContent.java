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

import javax.xml.bind.DatatypeConverter;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hpccsystems.spark.thor.FieldDef;

import scala.collection.JavaConverters;

/**
 * A sequence of binary objects, like SET OF DATA in ECL.
 */
public class BinarySeqContent extends Content implements Serializable {
  private static final long serialVersionUID = 1L;
  private boolean isAll;
  private byte[][] value;

  /**
   * Empty constructor for serialization.
   */
  protected BinarySeqContent() {
    this.value = new byte[0][];
    this.isAll = false;
  }

  /**
   * @param typ field type
   * @param name field name
   * @param v content value
   * @param f Universal set, all values
   */
  public BinarySeqContent(FieldType typ, String name, byte[][] v, boolean f) {
    super(FieldType.SET_OF_BINARY, name);
    this.value = new byte[v.length][];
    for (int i=0; i<v.length; i++) {
      this.value[i] = new byte[v[i].length];
      for (int j=0; j<v[i].length; j++) this.value[i][j] = v[i][j];
    }
    this.isAll = f;
  }

  /**
   * @param def
   * @param v content value
   * @param f Universal set, all values
   */
  public BinarySeqContent(FieldDef def, byte[][] v, boolean f) {
    super(def);
    if (def.getFieldType() != FieldType.SET_OF_BINARY) {
      throw new IllegalArgumentException("Wrong field type in definition");
    }
    this.value = new byte[v.length][];
    for (int i=0; i<v.length; i++) {
      this.value[i] = new byte[v[i].length];
      for (int j=0; j<v[i].length; j++) this.value[i][j] = v[i][j];
    }
    this.isAll = f;
  }
  /**
   * The raw data content.
   * @return
   */
  public byte[][] asSetOfBinary() {
    byte[][] rslt = new byte[this.value.length][];
    for (int i=0; i<this.value.length; i++) {
      rslt[i] = new byte[this.value[i].length];
      for (int j=0; j<this.value[i].length; j++) {
        rslt[i][j] = this.value[i][j];
      }
    }
    return rslt;
  }
  /**
   * Is this the universe of values?
   * @return
   */
  public boolean isAllValues() { return isAll; }

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
    StringBuilder sb = new StringBuilder(100 + this.value.length*100);
    for (int i=0; i<this.value.length; i++) {
      if (i>0) sb.append(elementSep);
      sb.append(DatatypeConverter.printHexBinary(this.value[i]));
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
      rslt[i] = DatatypeConverter.printHexBinary(this.value[i]);
    }
    return rslt;
  }

  @Override
  public Object asRowObject(DataType dtyp) {
    DataType test = DataTypes.createArrayType(DataTypes.BinaryType);
    if (!test.sameType(dtyp)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected Array of BinaryType, given ");
      sb.append(dtyp.typeName());
      throw new IllegalArgumentException(sb.toString());
    }
    int elements = this.value.length;
    ArrayList<byte[]> work = new ArrayList<byte[]>(elements);
    for (int i=0; i<elements; i++) {
      byte[] elem = new byte[this.value[i].length];
      for (int j=0; j<this.value[i].length; j++) {
        elem[j] = this.value[i][j];
      }
      work.add(elem);
    }
    return JavaConverters.asScalaBufferConverter(work).asScala().seq();
  }

}
