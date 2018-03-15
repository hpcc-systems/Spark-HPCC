package org.hpccsystems.spark.thor;

import java.util.HashMap;
import java.util.Iterator;

import org.hpccsystems.spark.FieldType;

import java.util.ArrayList;
import java.io.Serializable;

import com.fasterxml.jackson.core.JsonToken;

public class TypeDef implements Serializable {
  static final long serialVersionUID = 1L;
  private FieldType type;
  private String typeName;
  private int len;
  private HpccSrcType src;
  private FieldDef[] struct;
  private boolean unsignedFlag;
  private boolean fixedLength;
  private int childLen;
  private FieldType childType;
  private HpccSrcType childSrc;
  // flag values from eclhelper.hpp RtlFieldTypeMask enum definition
  final private static short flag_unsigned = 256;
  final private static short flag_unknownsize = 1024;
  // type codes from rtlconst.hpp type_vals enum definition
  final private static short type_boolean = 0;
  final private static short type_int = 1;
  final private static short type_real = 2;
  final private static short type_string = 4;
  final private static short type_record = 13;
  final private static short type_varstring = 14;
  final private static short type_table = 20;
  final private static short type_set = 21;
  final private static short type_unicode = 31;
  final private static short type_varunicode = 33;
  final private static short type_utf8 = 41;
  final private static short type_uint = flag_unsigned + type_int;
  final private static short type_vrecord = flag_unknownsize + type_record;
  final private static short type_vtable = flag_unknownsize + type_table;
  final private static short type_vstring = flag_unknownsize + type_string;
  final private static short type_vset = flag_unknownsize + type_set;
  final private static short type_vunicode = flag_unknownsize + type_unicode;
  final private static short type_vutf8 = flag_unknownsize + type_utf8;
  // JSON pair names of interest
  final private static String fieldTypeName = "fieldType";
  final private static String lengthName = "length";
  final private static String childName = "child";
  final private static String fieldsName = "fields";
  /**
   * Private constructor for dummy child object and serialization support.
   */
  protected TypeDef() {
    this.typeName = "none";
    this.len = 0;
    this.src = HpccSrcType.UNKNOWN;
    this.type = FieldType.MISSING;
    this.struct = new FieldDef[0];
    this.childLen = 0;
    this.childSrc = HpccSrcType.UNKNOWN;
    this.childType = FieldType.MISSING;
    this.unsignedFlag = false;
    this.fixedLength = false;
  }
  /**
   * Normal constructor to use when this definition describes a structure
   * @param type the value of the JSON pair named fieldType
   * @param typeName the name of the object for type objects
   * @param len the value of the JSON pair named length
   * @param childType the type of the child or MISSING if no child JSON pair
   * @param child length or 0 if no child JSON pair
   * @param child source enum type value
   * @param defs the field definitions specified by fields JSON pair
   */
  public TypeDef(long type_id, String typeName, int len,
      FieldType childType, int childLen, HpccSrcType childSrc,
      FieldDef[] defs) {
    short type = (type_id < 10000) ? (short) type_id   : -1;
    this.typeName = typeName;
    this.len = len;
    this.struct = defs;
    this.unsignedFlag = type==type_uint;
    this.childLen = childLen;
    this.childSrc = childSrc;
    this.childType = childType;
    switch (type) {
      case type_boolean:
        this.fixedLength = true;
        this.type = FieldType.BOOLEAN;
        break;
      case type_int:
      case type_uint:
        this.fixedLength = true;
        this.type = FieldType.INTEGER;
        break;
      case type_real:
        this.fixedLength = true;
        this.type = FieldType.REAL;
        break;
      case type_string:
      case type_unicode:
        this.fixedLength = true;
        this.type = FieldType.STRING;
        break;
      case type_varstring:
      case type_varunicode:
      case type_vstring:
      case type_vunicode:
      case type_vutf8:
      case type_utf8:
        this.fixedLength = false;
        this.type = FieldType.STRING;
        break;
      case type_set:
        this.fixedLength = true;
        switch (childType) {
          case INTEGER:
            this.type = FieldType.SET_OF_INTEGER;
            break;
          case REAL:
            this.type = FieldType.SET_OF_REAL;
            break;
          case BOOLEAN:
            this.type = FieldType.SET_OF_BOOLEAN;
            break;
          case STRING:
            this.type = FieldType.SET_OF_STRING;
            break;
          default:
            this.type = FieldType.SET_OF_MISSING;
        }
        break;
      case type_vset:
        this.fixedLength = false;
        switch (childType) {
          case INTEGER:
            this.type = FieldType.SET_OF_INTEGER;
            break;
          case REAL:
            this.type = FieldType.SET_OF_REAL;
            break;
          case BOOLEAN:
            this.type = FieldType.SET_OF_BOOLEAN;
            break;
          case STRING:
            this.type = FieldType.SET_OF_STRING;
            break;
          default:
            this.type = FieldType.SET_OF_MISSING;
        }
        break;
      case type_record:
        this.fixedLength = true;
        this.type = FieldType.RECORD;
        break;
      case type_vrecord:
        this.fixedLength = false;
        this.type = FieldType.RECORD;
        break;
      case type_table:
      case type_vtable:
        this.type = FieldType.SEQ_OF_RECORD;
        break;
      default:
        this.fixedLength = false;
        this.type = FieldType.MISSING;
    }
    switch (type) {
      case type_int:
      case type_uint:
      case type_real:
        this.src = HpccSrcType.LITTLE_ENDIAN;
        break;
      case type_utf8:
      case type_vutf8:
        this.src = HpccSrcType.UTF8;
        break;
      case type_string:
      case type_varstring:
      case type_vstring:
        this.src = HpccSrcType.SINGLE_BYTE_CHAR;
        break;
      case type_unicode:
      case type_vunicode:
        this.src = HpccSrcType.UTF16LE;
        break;
      case type_set:
      case type_vset:
        this.src = this.childSrc;
        break;
      default:
        this.src = HpccSrcType.UNKNOWN;
    }
  }
  /**
   * Use for type definition that has a child defined
   * @param type the value of the JSON pair named fieldType
   * @param typeName the name of the object for type objects
   * @param len the value of the JSON pair named length
   * @param fields the list of field definitions if this is a structure
   */
  public TypeDef(long type_id, String typeName, int len, FieldDef[] fields) {
    this(type_id, typeName, len, FieldType.MISSING, 0,
        HpccSrcType.UNKNOWN, fields);
  }
  /**
   * The type of this field definition.
   * @return FieldType enumeration value
   */
  public FieldType getType() { return type; }
  /**
   * The type name for this type
   * @return type name
   */
  public String getTypeName() { return typeName;  }
  /**
   * The description of this type.  Either the type name or
   * the description of the underlying FieldType.
   * @return a descriptive name of the type
   */
  public String description() {
    return type.description();
  }
  /**
   * The fixed length for this type or 0 if the length is variable
   * @return the fixed length or zero
   */
  public int getLength() { return len; }
  /**
   * The list of fields that define a structure type
   * @return the filed list or empty array
   */
  public FieldDef[] getStructDef() {
    FieldDef[] rslt = new FieldDef[struct.length];
    for (int i=0; i<struct.length; i++) rslt[i] = struct[i];
    return rslt;
  }
  /**
   * Is the numeric value an unsigned value?
   */
  public boolean isUnsigned() { return unsignedFlag; }
  /**
   * Is the field a fixed length?
   */
  public boolean isFixedLength() { return fixedLength; }
  /**
   * The type defined by the child pair if present or MISSING
   */
  public FieldType childType() { return childType; }
  /**
   * The fixed length of the child type or zero
   */
  public int childLen() { return childLen; }
  /**
   * The binary encoding type of the data source.
   * @return source type
   */
  public HpccSrcType getSourceType() { return this.src; }
  /**
   * Pick up a type definition from parsing a JSON string.  The type
   * definitions are object pairs.  The type definition object has pair
   * members named fieldType, length, child, and fields.  A fields pair
   * is an array of field definitions.
   * @param curr the START_OBJECT token that starts the definition
   * @param toks_iter iterator of the tokens making up a JSON string
   * @param type_dict a dictionary of the types previously defined
   * @return the type definition from the tokens
   */
  public static TypeDef parseDef(DefToken first, Iterator<DefToken> toks_iter,
      HashMap<String, TypeDef> type_dict)
    throws UnusableDataDefinitionException {
    if (first.getToken() != JsonToken.START_OBJECT) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected object start, found ");
      sb.append(first.toString());
      throw new UnusableDataDefinitionException(sb.toString());
    }
    if (!toks_iter.hasNext()) {
      throw new UnusableDataDefinitionException("Early termination");
    }
    String typeName = first.getName();
    long fieldType = 0;
    int length = 0;
    FieldType childType = FieldType.MISSING;
    int childLen = 0;
    HpccSrcType childSrc = HpccSrcType.UNKNOWN;
    FieldDef[] fields = new FieldDef[0];
    DefToken curr = toks_iter.next();
    while (toks_iter.hasNext() && curr.getToken() != JsonToken.END_OBJECT) {
      if (fieldTypeName.equals(curr.getName())) {
        fieldType = curr.getInteger();
      } else if (lengthName.equals(curr.getName())) {
        long rawLength = curr.getInteger();
        if (rawLength > Integer.MAX_VALUE || rawLength < 0) rawLength = 0;
        length = (int) rawLength;
      } else if (childName.equals(curr.getName())) {
        String name = curr.getString();
        if (!type_dict.containsKey(name)) {
          StringBuilder sb = new StringBuilder();
          sb.append("Type name ");
          sb.append(name);
          sb.append(" used but not defined.");
          throw new UnusableDataDefinitionException(sb.toString());
        }
        TypeDef child = type_dict.get(name);
        childType = child.getType();
        childLen = child.getLength();
        childSrc = child.getSourceType();
        fields = child.getStructDef();
      } else if (fieldsName.equals(curr.getName())) {
        if (curr.getToken() != JsonToken.START_ARRAY) {
          StringBuilder sb = new StringBuilder();
          sb.append("Expected an array for fields, found ");
          sb.append(curr.getToken().toString());
          throw new UnusableDataDefinitionException(sb.toString());
        }
        if (!toks_iter.hasNext()) {
          throw new UnusableDataDefinitionException("Early termination");
        }
        ArrayList<FieldDef> field_list = new ArrayList<FieldDef>();
        curr = toks_iter.next();
        while (curr.getToken() != JsonToken.END_ARRAY
              && toks_iter.hasNext()) {
          FieldDef fieldDef = FieldDef.parseDef(curr, toks_iter, type_dict);
          field_list.add(fieldDef);
          curr = toks_iter.next();
        }
        fields = field_list.toArray(new FieldDef[0]);
        if (!toks_iter.hasNext()) {
          throw new UnusableDataDefinitionException("Early termination");
        }
      }  // ignore unknown pairs
      curr = toks_iter.next();
    }
    if (!toks_iter.hasNext()) {
      new UnusableDataDefinitionException("Early object termination");
    }
    TypeDef rslt = new TypeDef(fieldType, typeName, length, childType,
        childLen, childSrc, fields);
    return rslt;
  }
}
