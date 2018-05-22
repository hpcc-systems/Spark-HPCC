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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.hpccsystems.spark.thor.FieldDef;

/**
 * The row content, an group of Content objects.
 */
public class RecordContent extends Content implements Serializable{
  final private static long serialVersionUID = 1L;
  private Content[] items;
  /**
   * Empty constructor for serialization
   */
  protected RecordContent() {
    this.items = new Content[0];
  }
  /**
   * Make a deep copy of the source object
   * @param src the content to be copied
   */
  protected RecordContent(RecordContent src) {
    this.items = new Content[src.items.length];
    for (int i=0; i<src.items.length; i++) this.items[i] = src.items[i];
  }
  /**
   * Constructor used when a FieldDef is not available,
   * @param name
   * @param fields the content fields
   */
  public RecordContent(String name, Content[] fields) {
    super(FieldType.RECORD, name);
    this.items = new Content[fields.length];
    for (int i=0; i<fields.length; i++) this.items[i] = fields[i];
  }
  /**
   * Normally used constructor.
   * @param def the fieldDef for the record
   * @param fields the record content fields
   */
  public RecordContent(FieldDef def, Content[] fields) {
    super(def);
    if (def.getFieldType()!=FieldType.RECORD) {
      throw new IllegalArgumentException("Type of def must be record");
    }
    this.items = new Content[fields.length];
    for (int i=0; i<fields.length; i++) this.items[i] = fields[i];
  }
  /**
   * As a copy of the fields.
   * @return
   */
  public Content[] asFieldArray() {
    Content[] rslt = new Content[this.items.length];
    for (int i=0; i<this.items.length; i++) rslt[i] = this.items[i];
    return rslt;
  }
  /**
   * Get the content item at position ndx in the array
   * @param ndx
   * @return
   */
  public Content fieldAt(int ndx) {
    return this.items[ndx];
  }
  /**
   * An iterator for the Content fields.
   * @return the iterator
   */
  public java.util.Iterator<Content> asIterator() {
    Content[] t = this.items;
    return new java.util.Iterator<Content>() {
      private final Content[] cp = t;
      private int pos = 0;
      public boolean hasNext() { return pos<cp.length; }
      public Content next() throws java.util.NoSuchElementException {
        if (pos >= cp.length) throw new java.util.NoSuchElementException();
        return cp[pos++];
      }
    };
  }
  protected Row asRow(StructType t){
    Object[] fields = new Object[this.items.length];
    for (int fld=0; fld<this.items.length; fld++) {
      if (!this.items[fld].getName().equals(t.apply(fld).name())) {
        StringBuilder sb = new StringBuilder();
        sb.append("Name mismatch at index ");
        sb.append(fld);
        sb.append("; ");
        sb.append(this.items[fld].getName());
        sb.append("!=");
        sb.append(t.apply(fld).name());
        throw new IllegalArgumentException(sb.toString());
      }
      fields[fld] = this.items[fld].asRowObject(t.apply(fld).dataType());
    }
    return new HpccRow(fields, t);
  }
  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#numFields()
   */
  @Override
  public int numFields() {
    return items.length;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asString()
   */
  @Override
  public String asString(String fieldSep, String elementSep) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<items.length; i++) {
      if (i>0) sb.append(fieldSep);
      sb.append(items[i].toString());
    }
    return sb.toString();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = this.asString();
    return rslt;
  }
  @Override
  public Object asRowObject(DataType dtyp) {
    if (!(dtyp instanceof StructType)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected structure type, given ");
      sb.append(dtyp.typeName());
      throw new IllegalArgumentException(sb.toString());
    } // field by field checks done during row build
    return this.asRow((StructType)dtyp);
  }

}
