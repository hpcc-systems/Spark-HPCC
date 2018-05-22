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

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A data record from the HPCC system.  A collection of fields, accessed by
 * name or as an enumeration.
 *
 *
 */
public class Record implements java.io.Serializable {
  static final long serialVersionUID = 1L;
  private String fileName;
  private int part;
  private long pos;
  private Content[] fieldContent;
  private java.util.HashMap<String, Content> content;
  /**
   * Record from an array of content items
   * @param content
   * @param fileName name of the file
   * @param part the file part number
   * @param pos the position of this record in the file part
   */
  public Record(Content[] fields, String fileName, int part, long pos) {
    this.fileName = fileName;
    this.part = part;
    this.pos = pos;
    this.content = new java.util.HashMap<String, Content>(fields.length*2);
    for (Content w : fields) {
      this.content.put(w.getName(), w);
    }
    this.fieldContent = new Content[fields.length];
    for (int i=0; i<fields.length; i++) this.fieldContent[i] = fields[i];
  }
  /**
   * No argument constructor for serialization support
   */
  protected Record() {
    this.fileName = "";
    this.content = new java.util.HashMap<String, Content>(0);
    this.fieldContent = new Content[0];
  }
  /**
   * Construct a labeled point record from an HPCC record.
   * @param labelName name of the content field to use as a label
   * @param dimNames names of the fields to use as the vector dimensions,
   * in the order specified.
   * @return a lebeled point record
   * @throws java.lang.IllegalArgumentException when a field name does not exist
   */
  public LabeledPoint asLabeledPoint(String labelName, String[] dimNames)
        throws IllegalArgumentException {
    if (!content.containsKey(labelName)) {
      throw new IllegalArgumentException(labelName + " not a record field.");
    }
    Content cv = content.get(labelName);
    double label = cv.getRealValue();
    Vector features = this.asMlLibVector(dimNames);
    LabeledPoint rslt = new LabeledPoint(label, features);
    return rslt;
  }
  /**
   * Create a Spark Row object for use in data frames.  An ArrayList
   * is used for sequences and sets.  A RecordContent item is a Row.
   * @param fd the field definition.  Must match the content.
   * @return the fields in order as language objects
   */
  public Row asRow(RecordDef rd) {
    Object[] fields = new Object[this.fieldContent.length];
    StructType schema = rd.asSchema();
    for (int i=0; i<this.fieldContent.length; i++) {
      StructField sfld = schema.apply(i);
      if (!sfld.name().equals(this.fieldContent[i].getName())) {
        StringBuilder sb = new StringBuilder();
        sb.append("Expected field name of ");
        sb.append(this.fieldContent[i].getName());
        sb.append(", given ");
        sb.append(sfld.name());
        throw new IllegalArgumentException(sb.toString());
      }
      fields[i] = this.fieldContent[i].asRowObject(sfld.dataType());
    }
    HpccRow rslt = new HpccRow(fields, schema);
    return rslt;
  }
  /**
   * Construct an MLLib dense vector from the HPCC record.
   * @param dimNames the names of the fields to use as the dimensions
   * @return a dense vector
   * @throws java.lang.IllegalArgumentException when the field name does not exist
   */
  public Vector asMlLibVector(String[] dimNames)
        throws java.util.NoSuchElementException {
    double[] features = new double[dimNames.length];
    for (int i=0; i<dimNames.length; i++) {
      if (!content.containsKey(dimNames[i])) {
        throw new IllegalArgumentException(dimNames[i] + " not a record field");
      }
      Content cv = content.get(dimNames[i]);
      features[i] = cv.getRealValue();
    }
    Vector rslt = Vectors.dense(features);
    return rslt;
  }
  /**
   * Get a field by the name.
   * @param name
   * @return
   */
  public Content getFieldContent(String name) {
    return content.get(name);
  }
  /**
   * Copy the record fields into an array
   * @return an array of content items
   */
  public Content[] getFields() {
    java.util.Collection<Content> w = content.values();
    Content[] rslt = w.toArray(new Content[0]);
    return rslt;
  }
  /**
   * The name of this file
   * @return the name
   */
  public String fileName() { return this.fileName; }
  /**
   * @return the part number of this part of the file
   */
  public int getFilePart() { return this.part; }
  /**
   * The relative record position of this record within the file part
   * @return record position
   */
  public long getPos() { return pos; }
  /**
   * A form of the record for display.
   * @return display string
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Record at position ");
    sb.append(this.getFilePart());
    sb.append(":");
    sb.append( Long.toString(this.getPos()));
    sb.append(" = ");
    for (Content fld : this.getFields()) sb.append(fld.toString());
    return sb.toString();
  }
}
