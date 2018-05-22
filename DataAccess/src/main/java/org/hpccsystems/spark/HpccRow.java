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
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class HpccRow implements Row, Serializable {
  static private final long serialVersionUID = 1L;
  private Object[] fieldList;
  private StructType schema;
  private HashMap<String,Integer> fieldMap;
  //
  /**
   * Null constructor used by serialization machinery
   */
  protected HpccRow() {
    this.schema = null;
    this.fieldList = null;
    this.fieldMap = null;
  }
  /**
   * Construct a row from an array of objects.  Creates a new array
   * but does not perform a deep copy.
   * @param rowFields
   * @param rowSchema
   */
  public HpccRow(Object[] rowFields, StructType rowSchema) {
    this.schema = rowSchema;
    this.fieldList = new Object[rowFields.length];
    this.fieldMap = new HashMap<String,Integer>(2*rowFields.length);
    String[] fieldNames = rowSchema.fieldNames();
    for (int i=0; i<rowFields.length; i++) {
      this.fieldList[i] = rowFields[i];
      fieldMap.put(fieldNames[i], new Integer(i));
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    String[] fieldNames = this.schema.fieldNames();
    for (int i=0; i<this.fieldList.length; i++) {
      if (i>0) sb.append(", ");
      sb.append(fieldNames[i]);
      sb.append(":");
      sb.append(this.fieldList[i].toString());
    }
    return sb.toString();
  }
  @Override
  public boolean anyNull() {
    boolean hasNull = false;
    for (int i=0; i<this.fieldList.length; i++) {
      if (this.fieldList[i]==null) {
        hasNull = true;
        break;
      }
    }
    return hasNull;
  }

  @Override
  public Object apply(int arg0) {
    return this.fieldList[arg0];
  }

  @Override
  public Row copy() {
    return new HpccRow(this.fieldList, this.schema);
  }

  @Override
  public int fieldIndex(String arg0) {
    Integer x = fieldMap.get(arg0);
    if (x==null) throw new IllegalArgumentException(arg0 + " not a field");
    return x.intValue();
  }

  @Override
  public Object get(int arg0) {
    return this.fieldList[arg0];
  }

  @Override
  public <T> T getAs(int arg0) {
    return (T) this.fieldList[arg0];
  }

  @Override
  public <T> T getAs(String arg0) {
    Integer x = fieldMap.get(arg0);
    if (x==null) throw new IllegalArgumentException(arg0 + " not a field");
    return (T) fieldList[x.intValue()];
  }

  @Override
  public boolean getBoolean(int arg0) {
    if (!(this.fieldList[arg0] instanceof Boolean)) {
      throw new ClassCastException();
    }
    return ((Boolean)this.fieldList[arg0]).booleanValue();
  }

  @Override
  public byte getByte(int arg0) {
    throw new ClassCastException();
  }

  @Override
  public Date getDate(int arg0) {
    throw new ClassCastException();
  }

  @Override
  public BigDecimal getDecimal(int arg0) {
    throw new ClassCastException();
  }

  @Override
  public double getDouble(int arg0) {
    double rslt = 0.0;
    if (this.fieldList[arg0] instanceof Number) {
      rslt = ((Number)this.fieldList[arg0]).doubleValue();
    } else throw new ClassCastException();
    return rslt;
  }

  @Override
  public float getFloat(int arg0) {
    float rslt = 0.0f;
    if (this.fieldList[arg0] instanceof Number) {
      rslt = ((Number)this.fieldList[arg0]).floatValue();
    } else throw new ClassCastException();
    return rslt;
  }

  @Override
  public int getInt(int arg0) {
    int rslt;
    if (this.fieldList[arg0] instanceof Number) {
      rslt = ((Number)this.fieldList[arg0]).intValue();
    } else throw new ClassCastException();
    return rslt;
  }

  @Override
  public <K, V> Map<K, V> getJavaMap(int arg0) {
    throw new ClassCastException("Not a map");
  }

  @Override
  public <T> List<T> getList(int arg0) {
    List<T> rslt;
    if(this.fieldList[arg0] instanceof Seq<?>) {
      int len = ((Seq<?>)this.fieldList[arg0]).size();
      Seq<Object> work = (Seq<Object>)this.fieldList[arg0];
      rslt = new ArrayList<T>(len);
      for (int i=0; i<len; i++) rslt.add((T)work.apply(i));
    } else throw new ClassCastException("Not an array type");
    return rslt;
  }

  @Override
  public long getLong(int arg0) {
    if (!(this.fieldList[arg0] instanceof Number)) {
      throw new ClassCastException();
    }
    return ((Number)(this.fieldList[arg0])).longValue();
  }

  @Override
  public <K, V> scala.collection.Map<K, V> getMap(int arg0) {
    throw new ClassCastException("Not a map");
  }

  @Override
  public <T> Seq<T> getSeq(int arg0) {
    if (!(this.fieldList[arg0] instanceof Seq<?>)) {
      throw new ClassCastException("Not an array field");
    }
    return (Seq<T>)(this.fieldList[arg0]);
  }

  @Override
  public short getShort(int arg0) {
    if (!(this.fieldList[arg0] instanceof Number)) {
      throw new ClassCastException();
    }
    return ((Number)(this.fieldList[arg0])).shortValue();
  }

  @Override
  public String getString(int arg0) {
    if (!(this.fieldList[arg0] instanceof String)) {
      throw new ClassCastException();
    }
    return (String)(this.fieldList[arg0]);
  }

  @Override
  public Row getStruct(int arg0) {
    if (!(this.fieldList[arg0] instanceof HpccRow)) {
      throw new ClassCastException();
    }
    return (Row)(this.fieldList[arg0]);
  }

  @Override
  public Timestamp getTimestamp(int arg0) {
    throw new ClassCastException();
  }

  @Override
  public <T> scala.collection.immutable.Map<String, T> getValuesMap(Seq<String> arg0) {
    List<String> nList = JavaConverters.seqAsJavaListConverter(arg0).asJava();
    ArrayList<Tuple2<String, T>> wrk = new ArrayList<>();
    for (String name : nList) {
      if (!this.fieldMap.containsKey(name)) {
        throw new IllegalArgumentException(name + " not a field name");
      }
      Object obj = this.fieldList[this.fieldMap.get(name).intValue()];
      wrk.add(new Tuple2(name, (T)obj));
    }
    Seq<Tuple2<String, T>> s
          = JavaConverters.asScalaBufferConverter(wrk).asScala().seq();
    return (scala.collection.immutable.Map<String, T>)
        scala.collection.immutable.Map$.MODULE$.apply(s);
  }

  @Override
  public boolean isNullAt(int arg0) {
    return this.fieldList[arg0] == null;
  }

  @Override
  public int length() {
    return this.fieldList.length;
  }

  @Override
  public String mkString() {
    return this.mkString("", "", "");
  }

  @Override
  public String mkString(String arg0) {
    return this.mkString("", arg0, "");
  }

  @Override
  public String mkString(String arg0, String arg1, String arg2) {
    StringBuilder sb = new StringBuilder();
    if (!("".equals(arg0))) sb.append(arg0);
    for (int i=0; i<this.fieldList.length; i++) {
      if (i>0 && (!("".equals(arg1)))) sb.append(arg1);
      if (this.fieldList[i] instanceof HpccRow) {
        sb.append(((HpccRow)this.fieldList[i]).mkString(arg1));
      } else if (this.fieldList[i] instanceof Seq<?>) {
        List<?> listFld
            = JavaConverters.seqAsJavaListConverter((Seq<?>)this.fieldList[i])
                .asJava();
        for (int j=0; j<listFld.size(); j++) {
          if (listFld.get(j) instanceof Seq<?>) {
            sb.setLength(0);    // clear the builder
            sb.append("Array of an Array data element encountered.");
            throw new UnsupportedOperationException(sb.toString());
          } else if (listFld.get(j) instanceof HpccRow) {
            sb.append(((HpccRow)listFld.get(j)).mkString(arg1));
          } else sb.append(listFld.get(j).toString());
        }
      } else sb.append(fieldList[i].toString());
    }
    if (!("".equals(arg2))) sb.append(arg2);
    return sb.toString();
  }

  @Override
  public StructType schema() {
    return new StructType(this.schema.fields());
  }

  @Override
  public int size() {
    return this.fieldList.length;
  }

  @Override
  public Seq<Object> toSeq() {
    ArrayList<Object> work = new ArrayList<Object>(this.fieldList.length);
    for (int i=0; i<this.fieldList.length; i++) {
      work.add(this.fieldList[i]);
    }
    return JavaConverters.asScalaBufferConverter(work).asScala().seq();
  }

}
