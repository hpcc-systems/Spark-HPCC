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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hpccsystems.spark.thor.DefToken;
import org.hpccsystems.spark.thor.FieldDef;
import org.hpccsystems.spark.thor.HpccSrcType;
import org.hpccsystems.spark.thor.TypeDef;
import org.hpccsystems.spark.thor.UnusableDataDefinitionException;

/**
 * HPCC record definition.  Includes HPCC record info strings and derived
 * Field Defs.
 *
 */
public class RecordDef implements Serializable {
  private static final long serialVersionUID = 1l;
  private String input_def;
  private FieldDef root;
  private String output_def;
  // flag values from eclhelper.hpp RtlFieldTypeMask enum definition
  final private static short flag_unknownsize = 1024;
  // type codes from rtlconst.hpp type_vals enum definition
  final private static short type_record = 13;
  final private static short type_vrecord = flag_unknownsize + type_record;
  // special constants
  private static final String fieldListName = "fields";
  private static final String fieldTypeName = "fieldType";
  private static final String fieldLengthName = "length";
  private static final String childName = "child";
  /**
   * Empty constructor for serialization
   */
  protected RecordDef() {
    this.root = null;
    this.output_def = "";
    this.input_def = "";
  }
  /**
   * Construct a record definition.  Normally used by the static
   * function parseJsonDef.
   * @param defThor the Json string used as the input definition for
   * the data on THOR.
   * @param defContent the Json string used to define the data to be
   * sent to this client.
   * @param root the definition parsed into FieldDef objects.  The input
   * is the root definition for the record.
   */
  public RecordDef(String defThor, String defContent, FieldDef root) {
    this.input_def = defThor;
    this.output_def = defContent;
    this.root = root;
  }
  /**
   * Create a record definition object from the JSON definition
   * string.  We have a type definition object composed by one or
   * more type definition object pairs.  The top level type definition
   * has fieldType, length, and fields pairs.
   * @param defThor the JSON record type definition returned from WsDfu
   * @param cp a column pruner for selecting specific columns of datga
   * @return a new record definition
   */
  static public RecordDef fromJsonDef(String defThor, ColumnPruner cp)
      throws UnusableDataDefinitionException {
    DefToken[] toks = new DefToken[0];
    try {
      toks = DefToken.parseDefString(defThor);
    } catch (JsonParseException e) {
      throw new UnusableDataDefinitionException("Failed to parse def", e);
    }
    toks = cp.pruneDefTokens(toks);
    StringBuilder def_sb = new StringBuilder();
    Iterator<DefToken> toks_iter = Arrays.asList(toks).iterator();
    while (toks_iter.hasNext()) {
      def_sb.append(toks_iter.next().toJson());
    }
    String defContent = def_sb.toString();
    toks_iter = Arrays.asList(toks).iterator();
    HashMap<String, TypeDef> types = new HashMap<String, TypeDef>();
    ArrayList<FieldDef> record_fields = new ArrayList<FieldDef>();
    // have an unnamed type definition object with 1 or more pairs of
    // type definition objects followed by a fieldType pair of Record Type,
    // a length pair, and a fields array pair of fields of the record
    if (!toks_iter.hasNext()) {
      throw new UnusableDataDefinitionException("Empty definition");
    }
    long len = 0;
    long type_id = 0;
    DefToken curr = toks_iter.next();
    if (curr.getToken() != JsonToken.START_OBJECT
        || curr.getName() != null) {
      StringBuilder sb = new StringBuilder();
      sb.append("Illegal start of definition. ");
      if (curr.getName() != null) sb.append("Named pair of type ");
      if (curr.getToken() != JsonToken.START_OBJECT) {
        sb.append(curr.getToken().toString());
      }
      sb.append(" found.");
      throw new UnusableDataDefinitionException(sb.toString());
    }
    // pick up the type definitions
    if (!toks_iter.hasNext()) {
      throw new UnusableDataDefinitionException("Early termination.");
    }
    curr = toks_iter.next();
    while (curr.getToken()==JsonToken.START_OBJECT && toks_iter.hasNext()) {
      TypeDef typedef = TypeDef.parseDef(curr, toks_iter, types);
      types.put(typedef.getTypeName(), typedef);
      curr = toks_iter.next();
    }
    // pick up the field definitions, type, and length
    if (!toks_iter.hasNext()) {
      throw new UnusableDataDefinitionException("Early termination");
    }
    if (curr.getParent() != 0) {
      StringBuilder sb = new StringBuilder();
      sb.append("Unexpected content, not in the root object; found ");
      sb.append(curr.toString());
      throw new UnusableDataDefinitionException(sb.toString());
    }
    while (curr.getToken()!=JsonToken.END_OBJECT && toks_iter.hasNext()) {
      if (fieldListName.equals(curr.getName())
          && curr.getToken()==JsonToken.START_ARRAY) {
        curr = toks_iter.next();
        while (toks_iter.hasNext() && curr.getToken()!=JsonToken.END_ARRAY) {
          FieldDef fieldDef = FieldDef.parseDef(curr, toks_iter, types);
          record_fields.add(fieldDef);
          curr = toks_iter.next();
        }
        if (!toks_iter.hasNext()) {
          throw new UnusableDataDefinitionException("Early termination");
        }
      } else if (fieldTypeName.equals(curr.getName())) {
        type_id = curr.getInteger();
        if (type_id != type_record && type_id != type_vrecord) {
          StringBuilder sb = new StringBuilder();
          sb.append("Bad value for type in root, found ");
          sb.append(curr.toString());
          throw new UnusableDataDefinitionException(sb.toString());
        }
      } else if (fieldLengthName.equals(curr.getName())) {
        len = curr.getInteger();
      }
      curr = toks_iter.next();
    }
    if (curr.getParent() != -1 || curr.getToken() != JsonToken.END_OBJECT) {
      StringBuilder sb = new StringBuilder();
      sb.append("Unexpected end record definition, found ");
      sb.append(curr.toString());
      throw new UnusableDataDefinitionException(sb.toString());
    }
    if (toks_iter.hasNext()) {
      throw new UnusableDataDefinitionException("More tokens but at end");
    }
    // create record def
    FieldDef root = new FieldDef("root", FieldType.RECORD, "none",
        len, type_id==type_record, HpccSrcType.UNKNOWN,
        record_fields.toArray(new FieldDef[0]));
    RecordDef rslt = new RecordDef(defThor, defContent, root);
    return rslt;
  }
  /**
   * The definition of the data for the remote file reader
   * @return the definition
   */
  public String getJsonInputDef() { return input_def; }
  /**
   * Get the JSON string that defines the output structure for the remote
   * reader.
   * @return the output definition
   */
  public String getJsonOutputDef() { return output_def; }
  /**
   * The record definition object
   * @return root definition
   */
  public FieldDef getRootDef() { return root; }
  /**
   * String display of the record definition.
   * @return the definition in display form
   */
  public String toString() {
    return "RECORD: " + root.toString();
  }
  public StructType asSchema() {
    return this.root.asSchema();
  }
}
