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
/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hpccsystems.spark.thor.FieldDef;

import scala.collection.JavaConverters;

/**
 *
 */
public class StringSeqContent extends Content implements Serializable{
  private static final long serialVersionUID = 1L;
  private boolean isAll;
  private String[] values;
  /**
   * Empty constructor for serialization
   */
  protected StringSeqContent() {
    this.values = new String[0];
    this.isAll = false;
  }
  /**
   * Constructor without a field def, copies content array
   * @param name the name of the field
   * @param content the array of strings
   * @param f Universal set, all values
   */
  public StringSeqContent(String name, String[] content, boolean f) {
    super(FieldType.SET_OF_STRING, name);
    this.values = new String[content.length];
    for (int i=0; i<content.length; i++) this.values[i]=content[i];
    this.isAll = f;
  }
  /**
   * Normal public constructor.  Copies the array.
   * @param def the field definition
   * @param content
   * @param f Universal set, all values
   */
  public StringSeqContent(FieldDef def, String[] content, boolean f) {
    super(def);
    this.values = new String[content.length];
    for (int i=0; i<content.length; i++) this.values[i] = content[i];
    this.isAll = f;
  }
  /**
   * Is this a universe of values
   * @return
   */
  public boolean isAllValues() { return isAll; }

  @Override
  public int numFields() {
    return 1; // only 1 field!
  }
  @Override
  public String asString(String fieldSep, String elementSep) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<this.values.length; i++) {
      if (i>0) sb.append(elementSep);
      sb.append(this.values[i]);
    }
    return sb.toString();
  }
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[this.values.length];
    for (int i=0; i<this.values.length; i++) rslt[i] = this.values[i];
    return rslt;
  }
  @Override
  public Object asRowObject(DataType dtyp) {
    DataType test = DataTypes.createArrayType(DataTypes.StringType);
    if (!test.sameType(dtyp)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected array of string type, given ");
      sb.append(dtyp.typeName());
    }
    ArrayList<String> work = new ArrayList<String>(this.values.length);
    for (int i=0; i<this.values.length; i++) {
      work.add(this.values[i]);
    }
    return JavaConverters.asScalaBufferConverter(work).asScala().seq();
  }

}
