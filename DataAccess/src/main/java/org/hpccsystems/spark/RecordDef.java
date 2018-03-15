package org.hpccsystems.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;

import org.hpccsystems.spark.thor.DefToken;
import org.hpccsystems.spark.thor.FieldDef;
import org.hpccsystems.spark.thor.HpccSrcType;
import org.hpccsystems.spark.thor.TypeDef;
import org.hpccsystems.spark.thor.UnusableDataDefinitionException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DataType;

/**
 * HPCC record definition.  Includes HPCC record info strings and derived
 * Field Defs.
 * @author holtjd
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
   * @param def the Json string used as the input and default output
   * definition.
   * @param root the definition parsed into FieldDef objects.  The input
   * is the root definition for the record.
   */
  public RecordDef(String def, FieldDef root) {
    this.input_def = def;
    this.output_def = def;  // default output is all content
    this.root = root;
  }
  /**
   * Create a record definition object from the JSON definition
   * string.  We have a type definition object composed by one or
   * more type definition object pairs.  The top level type definition
   * has fieldType, length, and fields pairs.
   * @param def the JSON record type defintion returned from WsDfu
   * @return a new record definition
   */
  static public RecordDef parseJsonDef(String def)
      throws UnusableDataDefinitionException {
    ArrayList<DefToken> toks = new ArrayList<DefToken>();
    try {
      toks = DefToken.parseDefString(def);
    } catch (JsonParseException e) {
      throw new UnusableDataDefinitionException("Failed to parse def", e);
    }
    Iterator<DefToken> toks_iter = toks.iterator();
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
    long childLen = 0;
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
      } else if (childName.equals(curr.getName())) {
        String childTypeName = curr.getString();
        if (types.containsKey(childTypeName)) {
          childLen = types.get(childTypeName).childLen();
        }
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
        len, childLen, type_id==type_record, HpccSrcType.UNKNOWN,
        record_fields.toArray(new FieldDef[0]));
    RecordDef rslt = new RecordDef(def, root);
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
   * Replace the current output definition.
   * @param new_def the new definition, a JSON string
   * @return the prior definition
   */
  public String setJsonOutputDef(String new_def) {
    String old = this.output_def;
    this.output_def = new_def;
    return old;
  }
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
    StructField[] fields = new StructField[this.root.getNumDefs()];
    for (int i=0; i<this.root.getNumDefs(); i++) {
      fields[i] = this.root.getDef(i).asSchemaElement();
    }
    return DataTypes.createStructType(fields);
  }
}
