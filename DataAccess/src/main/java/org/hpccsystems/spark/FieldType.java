/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * The data types for data fields on an HPCC record.
 *
 * @author holtjd
 *
 */
public enum FieldType implements Serializable {
  INTEGER(true, "Integer", false),
  REAL(true, "Real", false),
  STRING(true, "String", false),
  BOOLEAN(true, "Boolean", false),
  BINARY(true, "Binary data", false),
  RECORD(false, "Record", true),
  MISSING(true, "Missing value", false),
  SET_OF_INTEGER(false, "Set of integers", false),
  SET_OF_REAL(false, "Set of reals", false),
  SET_OF_STRING(false, "Set of strings", false),
  SET_OF_BOOLEAN(false, "Set of Booleans", false),
  SET_OF_BINARY(false, "Set of Binary strings", false),
  SEQ_OF_RECORD(false, "Seq of records", true),
  SET_OF_MISSING(false, "Set of unknown", false);

  static final long serialVersionUID = 1L;
  private boolean scalar;
  private String name;
  private boolean composite;
  /**
   * A FieldType enumeration value.  Sets of primitives are not
   * atomic and not composites.  Records and Record Sets are not
   * atomic but are composites.  Primitives are atomic and not
   * composites.  The primitive types are Boolean, Integer, String,
   * and Real.
   *
   * The MISSING and SET_OF_MISSING occur when the type information
   * handled in the TypeDef class is not understood.  Possible
   * underlying types that are not understood include the Foreign
   * data types, QSTRING, bit fields, ECL ENUM.
   * @param atomicType is an atomic value, a primitive value
   * @param name the descriptive name of the this value
   * @param composite is this a structure
   */
  FieldType(boolean atomicType, String name, boolean composite) {
    this.scalar = atomicType;
    this.name = name;
    this.composite = composite;
  }
  /**
   * default constructor for serialization support.
   */
  FieldType() {
    this.scalar = true;
    this.name = "";
    this.composite = false;
  }
  /**
   * Is this a primitive scalar type
   */
  public boolean isScalar() { return this.scalar; }
  /**
   * Is this a set of scalars?
   */
  public boolean isVector() { return !this.scalar && !this.composite; }
  /**
   * Is this a record or a set of records
   */
  public boolean isComposite() { return this.composite; }
  /**
   * Description of the type.
   * @return a descriptive string
   */
  public String description() {
    return name;
  }
}
