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
package org.hpccsystems.spark.thor;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hpccsystems.spark.FieldType;

import com.fasterxml.jackson.core.JsonToken;

/**
 * The name and field type for an item from the HPCC environment.  The
 * types may be single scalar types or may be arrays or structures.
 *
 */

public class FieldDef implements Serializable {
  static final long serialVersionUID = 1L;
  private String fieldName = "";
  private FieldType fieldType = FieldType.UNKNOWN;
  private String typeName = FieldType.UNKNOWN.description();
  private FieldDef[] defs = new FieldDef[0];
  private HpccSrcType srcType = HpccSrcType.UNKNOWN;
  private int fields = 0;
  private int len = 0;
  private boolean fixedLength = false;
  private boolean isUnsigned = false;
  private StructField schemaElement = null;
  private StructType schema = null;
  //
  private static final String FieldNameName = "name";
  private static final String FieldTypeName = "type";
  //
  protected FieldDef() {
  }
  /**
   * @param fieldName the name for the field or set or structure
   * @param fieldDef the type definition
   */
  public FieldDef(String fieldName, TypeDef typeDef) {
      this.fieldName = fieldName;
      this.fieldType = typeDef.getType();
      this.typeName = typeDef.description();
      this.defs = typeDef.getStructDef();
      this.srcType = typeDef.getSourceType();
      this.fields = this.defs.length;
      this.len = typeDef.getLength();
      this.fixedLength = typeDef.isFixedLength();
      this.isUnsigned = typeDef.isUnsigned();
  }
  /**
   * @param fieldName the name of the field
   * @param fieldType the FieldType value
   * @param typeName the name of this composite type
   * @param len the field length
   * @param isFixedLength len may be non-zero and variable
   * @param defs the array of fields composing this def
   */
  public FieldDef(String fieldName, FieldType fieldType, String typeName, long len,
      boolean isFixedLength, HpccSrcType styp, FieldDef[] defs) {
    if (len>Integer.MAX_VALUE) {
      StringBuilder sb = new StringBuilder();
      sb.append("Field length values too large for ");
      sb.append(fieldName);
      throw new IllegalArgumentException(sb.toString());
    }
    this.fieldName = fieldName;
    this.fieldType = fieldType;
    this.typeName = typeName;
    this.defs = defs;
    this.srcType = styp;
    this.fields = defs.length;
    this.fixedLength = isFixedLength;
    this.len = (int) len;
  }
  /**
   * the name of the field
   * @return the name
   */
  public String getFieldName() {
    return fieldName;
  }
  /**
   * the type of the field using the FieldType ENUM type.
   * @return the type as an enumeration value
   */
  public FieldType getFieldType() {
    return fieldType;
  }
  /**
   * Data type on the HPCC cluster.
   * @return type enumeration
   */
  public HpccSrcType getSourceType() { return this.srcType; }
  /**
   * Length of the data or minimum length if variable
   * @return length
   */
  public int getDataLen() { return this.len; }
  /**
   * Fixed or variable length
   * @return true when fixed length
   */
  public boolean isFixed() { return this.fixedLength; }
  /**
   * Is the underlying value type unsigned?
   * @return true when unsigned
   */
  public boolean isUnsigned() { return this.isUnsigned; }
  /**
   * Translates a FieldDef into a StructType schema
   * @return StructType
   */
  public StructType asSchema() {
    if (this.fieldType != FieldType.RECORD) {
      return null;
    }

    if (this.schema != null) {
      return this.schema;
    }

    StructField[] fields = new StructField[this.getNumDefs()];
    for (int i=0; i<this.getNumDefs(); i++) {
      fields[i] = this.getDef(i).asSchemaElement();
    }
    this.schema = DataTypes.createStructType(fields);
    return this.schema;
  }

  /**
   * translate a FieldDef into a StructField object of the schema
   * @return
   */
  public StructField asSchemaElement() {
    if (this.schemaElement != null) {
      return this.schemaElement;
    }

    Metadata empty = Metadata.empty();
    boolean nullable = false;

    DataType type = DataTypes.NullType;
    switch (this.fieldType) {
      case VAR_STRING:
      case STRING:
        type = DataTypes.StringType;
        break;
      case INTEGER:
        type = DataTypes.LongType;
        break;
      case BINARY:
        type = DataTypes.BinaryType;
        break;
      case BOOLEAN:
        type = DataTypes.BooleanType;
        break;
      case REAL:
        type = DataTypes.DoubleType;
        break;
      case DECIMAL:
        int precision = getDataLen() & 0xffff;
        int scale = getDataLen() >> 16;

        // Spark SQL only supports 38 digits in decimal values
        if (precision > DecimalType.MAX_PRECISION()) {
          scale -= (precision - DecimalType.MAX_PRECISION());
          if (scale < 0) {
            scale = 0;
          }
          
          precision = DecimalType.MAX_PRECISION();
        }

        type = DataTypes.createDecimalType(precision,scale);
        break;
      case SET:
      case DATASET:
        StructField childField = this.defs[0].asSchemaElement();
        type = DataTypes.createArrayType(childField.dataType());
        nullable = true;
        break;
      case RECORD:
        StructField[] childFields = new StructField[this.defs.length];
        for (int i=0; i<this.defs.length; i++) {
          childFields[i] = this.defs[i].asSchemaElement();
        }
        type = DataTypes.createStructType(childFields);
        break;
      case UNKNOWN:
        type = DataTypes.NullType;
        break;
    }

    this.schemaElement = new StructField(this.fieldName, type, nullable, empty);
    return this.schemaElement; 
  }
  /**
   * A descriptive string showing the name and type.  When the
   * type is a composite, the composite definitions are included.
   * @return the string value
   */
  public String toString() {
    StringBuffer sb = new StringBuffer(this.fields*20 + 10);
    sb.append("FieldDef [fieldName=");
    sb.append(this.fieldName);
    sb.append(", ");
    sb.append((this.fixedLength) ? "F len="  : "V len=");
    sb.append(len);
    sb.append(" ");
    sb.append(this.srcType.toString());
    sb.append(", fieldType=");
    if (this.fieldType.isComposite()) {
      sb.append("{");
      sb.append(this.fields);
      sb.append("}{");
      for (int i=0; i<this.defs.length; i++) {
        if (i>0) sb.append("; ");
        sb.append(this.defs[i].toString());
      }
      sb.append("}");
    } else sb.append(this.typeName);
    sb.append("]");
    return sb.toString();
  }
  /**
   * The type name based upon the type enum with decorations for
   * composites.
   *@return the name of the type
   */
  public String typeName() {
    return (this.fieldType.isScalar() || this.fieldType.isVector())
        ? this.typeName
        : "RECORD(" + this.typeName + ")";
  }
  /**
   * Record name if this is a composite field
   * @return a blank name.
   */
  public String recordName() {
    return (this.fieldType.isComposite()) ? this.typeName : "";
  }
  /**
   * The number of fields, 1 or more if a record
   * @return number of fields.
   */
  public int getNumFields() { return this.fields; }
  /**
   * Number of field definitions.  Zero if this is not a record
   * @return number
   */
  public int getNumDefs() { return this.defs.length; }
  /**
   * Get the FieldDef at position.  Will throw an array out of bounds
   * exception.
   * @param ndx index position
   * @return the FieldDef object
   */
  public FieldDef getDef(int ndx) { return this.defs[ndx]; }
  /**
   * An iterator to walk though the type definitions that compose
   * this type.
   * @return an iterator returning FieldDef objects
   */
  public Iterator<FieldDef> getDefinitions() {
    final FieldDef[] defRef = this.defs;
    Iterator<FieldDef> rslt = new Iterator<FieldDef>() {
      int pos = 0;
      FieldDef[] copy = defRef;
      public boolean hasNext() {
        return (pos<copy.length)  ? true  : false;
      }
      public FieldDef next() {
        return copy[pos++];
      }
    };
    return rslt;
  }
  /**
   * Pick up a field definition from the JSON record definiton string.
   * The definitions are objects in the fields JSON array pair.  The
   * objects have name, type name, flags, and xpath pairs.  The flags
   * and xpath pairs are ignored.
   *
   * Start with a START_OBJECT and return on an END_OBJECT.  An exception
   * is thrown if not true or if name or type pairs are missing.
   *
   * @param first the first token in the sequence, must be START_OBJECT.
   * @param toks_iter an itreator of the tokens from a JSON record def string
   * @param type_dict the dictionary of types defined earlier in the string
   * @return the field defintion
   */
  public static FieldDef parseDef(DefToken first,
            Iterator<DefToken> toks_iter,
            HashMap<String, TypeDef> type_dict)
      throws UnusableDataDefinitionException {
    if (first.getToken() != JsonToken.START_OBJECT) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected start of object, found ");
      sb.append(first.getToken().toString());
      throw new UnusableDataDefinitionException(sb.toString());
    }
    if (!toks_iter.hasNext()) {
      throw new UnusableDataDefinitionException("Early termination");
    }
    DefToken curr = toks_iter.next();
    String fieldName = "";
    String typeName = "";
    while(toks_iter.hasNext() && curr.getToken() != JsonToken.END_OBJECT) {
      if (FieldNameName.equals(curr.getName())) {
        fieldName = curr.getString();
      }
      if (FieldTypeName.equals(curr.getName())) {
        typeName = curr.getString();
      }
      curr = toks_iter.next();
    }
    if (!toks_iter.hasNext()) {
      throw new UnusableDataDefinitionException("Early termination");
    }
    if (fieldName.equals("") || typeName.equals("")) {
      throw new UnusableDataDefinitionException("Missing name or type pairs");
    }
    if (!type_dict.containsKey(typeName)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Type name ");
      sb.append(typeName);
      sb.append(" used but not defined.");
      throw new UnusableDataDefinitionException(sb.toString());
    }
    TypeDef typ = type_dict.get(typeName);
    FieldDef rslt = new FieldDef(fieldName, typ);
    return rslt;
  }
}
