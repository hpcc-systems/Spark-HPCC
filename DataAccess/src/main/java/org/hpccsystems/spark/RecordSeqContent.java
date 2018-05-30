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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.hpccsystems.spark.thor.FieldDef;

import scala.collection.JavaConverters;

/**
 *
 */
public class RecordSeqContent extends Content implements Serializable {
  private static final long serialVersionUID = 1L;
  private RecordContent[] value;
  /**
   * Empty constructor for serialization
   */
  protected RecordSeqContent() {
    this.value = new RecordContent[0];
  }

  /**
   * @param name
   * @param v the array of RecordContent objects
   */
  public RecordSeqContent(String name, RecordContent[] v) {
    super(FieldType.SEQ_OF_RECORD, name);
    this.value = new RecordContent[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
  }

  /**
   * @param def
   * @param v the set of RecordContent values
   */
  public RecordSeqContent(FieldDef def, RecordContent[] v) {
    super(def);
    if (def.getFieldType() != FieldType.SEQ_OF_RECORD) {
      throw new IllegalArgumentException("Field Def has the wrong data type");
    }
    this.value = new RecordContent[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
  }
  public RecordContent[] asRecordSeqContent() {
    RecordContent[] rslt = new RecordContent[this.value.length];
    for (int i=0; i<this.value.length; i++) rslt[i] = this.value[i];
    return rslt;
  }
  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#numFields()
   */
  @Override
  public int numFields() {
    // TODO Auto-generated method stub
    return 1;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asString(java.lang.String, java.lang.String)
   */
  @Override
  public String asString(String fieldSep, String elementSep) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<this.value.length; i++) {
      if (i>0) sb.append(elementSep);
      sb.append(this.value[i].asString(fieldSep, elementSep));
    }
    return sb.toString();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object asRowObject(DataType dtyp) {
    if (!(dtyp instanceof ArrayType)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected array of records, given ");
      sb.append(dtyp.typeName());
      throw new IllegalArgumentException(sb.toString());
    }
    StructType styp = (StructType)((ArrayType)dtyp).elementType();
    ArrayList<Row> work = new ArrayList<Row>(this.value.length);
    for (int i=0; i<this.value.length; i++) {
      work.add(this.value[i].asRow(styp));
    }
    return JavaConverters.asScalaBufferConverter(work).asScala().seq();
  }

}
