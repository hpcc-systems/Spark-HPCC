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
package org.hpccsystems.dafilesrv.spark.client;

import org.apache.spark.sql.types.*;

import java.util.Map;
import java.util.HashMap;


/** 
 * Helper class that extracts information relevant to writing data to HPCC from a Spark row schema
 */
public class SparkField {
    public enum FieldType {
        ARRAY_TYPE,
        BINARY_TYPE,
        BOOLEAN_TYPE,
        BYTE_TYPE,
        DECIMAL_TYPE,
        DOUBLE_TYPE,
        FLOAT_TYPE,
        INTEGER_TYPE,
        LONG_TYPE,
        SHORT_TYPE,
        STRING_TYPE,
        STRUCT_TYPE,
        UNKNOWN_TYPE;

        public static FieldType getByValue(DataType type) {
            if (type instanceof ArrayType) {
                return FieldType.ARRAY_TYPE;
            } else if (type instanceof BinaryType) {
                return FieldType.BINARY_TYPE;
            } else if (type instanceof BooleanType) {
                return FieldType.BOOLEAN_TYPE;
            } else if (type instanceof ByteType) {
                return FieldType.BYTE_TYPE;
            } else if (type instanceof DecimalType) {
                return FieldType.DECIMAL_TYPE;
            } else if (type instanceof DoubleType) {
                return FieldType.DOUBLE_TYPE;
            } else if (type instanceof FloatType) {
                return FieldType.FLOAT_TYPE;
            } else if (type instanceof IntegerType) {
                return FieldType.INTEGER_TYPE;
            } else if (type instanceof LongType) {
                return FieldType.LONG_TYPE;
            } else if (type instanceof ShortType) {
                return FieldType.SHORT_TYPE;
            } else if (type instanceof StringType) {
                return FieldType.STRING_TYPE;
            } else if (type instanceof StructType) {
                return FieldType.STRUCT_TYPE;
            }

            return FieldType.UNKNOWN_TYPE;
        }
    };

    public String name = "";
    public FieldType type = FieldType.UNKNOWN_TYPE;
    public int precision = 0;
    public int scale = 0;

    public SparkField[] children = null;
    
    /**
    * SparkField Constructor
    * Extracts information from the provided StructField relevant to 
    * converting Spark Schemas to HPCC record definitions.
    * @param field The StructField to extract information from. 
    * @throws Exception 
    */
    public SparkField(StructField field) throws Exception {
        this.type = FieldType.getByValue(field.dataType());
        this.name = field.name();

        if (this.type == FieldType.STRUCT_TYPE) {
            StructType schema = (StructType) field.dataType();
            StructField schemaFields[] = schema.fields();
            this.children = new SparkField[schemaFields.length];

            for (int i = 0; i < schemaFields.length; i++) {
                this.children[i] = new SparkField(schemaFields[i]);
            }
        } else if (this.type == FieldType.DECIMAL_TYPE) {
            DecimalType typeInfo = (DecimalType) field.dataType();
            this.precision = typeInfo.precision();
            this.scale = typeInfo.scale();

            int mostSigDigit = this.precision - this.scale;
            if (mostSigDigit > 32) {
                throw new Exception("Invalid decimal format with precision: " + this.precision
                                    + " and scale: " + this.scale 
                                    + ". Max of 32 digits above decimal point supported.");
            }
        } else if (this.type == FieldType.ARRAY_TYPE) {
            ArrayType typeInfo = (ArrayType) field.dataType();
            this.children = new SparkField[1];

            StructField tempField = new StructField(this.name,typeInfo.elementType(),
                                                    false,Metadata.empty());
            this.children[0] = new SparkField(tempField);
        } else if (this.type == FieldType.UNKNOWN_TYPE) {
            throw new Exception("Unable to map Spark field type: " 
                                + field.dataType().getClass().getName() 
                                + " to an HPCC field type.");
        }
    }

    /**
    * toECL 
    * Converts the provided SparkField into an ECL record definition
    * @param field the SparkField to convert
    * @return ECL Record defintion as a String
    * @throws Exception
    */
    public static String toECL(SparkField field) throws Exception {
        if (field.type != FieldType.STRUCT_TYPE) {
            throw new Exception("Invalid record structure. Root object must of type Struct");
        }

        // Recurse through the tree structure and generate record defintions
        HashMap<String,String> recordDefinitionMap = new HashMap<String,String>();
        String rootRecordName = getEClTypeDefinition(field, recordDefinitionMap);

        // Get root record definition and remove it from the map 
        String definition = recordDefinitionMap.get(rootRecordName);
        recordDefinitionMap.remove(rootRecordName);
        definition = definition.replace(rootRecordName, "RD");

        // Combine the type definitions into a single ECL defintion
        int numRecordDefinitions = 1;
        for (Map.Entry<String,String> entry : recordDefinitionMap.entrySet()) {
           
            definition = entry.getKey() + " := "
                       + entry.getValue() + "\n\n" 
                       + definition;

            // Replace the temporary hash key with something more readable
            definition = definition.replace(entry.getKey(), "RD" + numRecordDefinitions);
            numRecordDefinitions++;
        }

        return definition;
    }

    private static String getEClTypeDefinition(SparkField field, Map<String,String> recordDefinitionMap) throws Exception
    {
        switch (field.type) {
            case ARRAY_TYPE: {
                if (field.children[0].type != SparkField.FieldType.STRUCT_TYPE) {
                    return "SET OF " + getEClTypeDefinition(field.children[0],recordDefinitionMap);
                } else {
                    return "DATASET(" + getEClTypeDefinition(field.children[0],recordDefinitionMap) + ")";
                }
            }
            case BINARY_TYPE: {
                return "DATA";
            }
            case BOOLEAN_TYPE: {
                return "BOOLEAN";
            }
            case BYTE_TYPE: {
                return "INTEGER1";
            }
            case DECIMAL_TYPE: {
                return "DECIMAL" + field.precision + "_" + field.scale;
            }
            case DOUBLE_TYPE: {
                return "REAL8";
            }
            case FLOAT_TYPE: {
                return "REAL4";
            }
            case INTEGER_TYPE: {
                return "INTEGER4";
            }
            case LONG_TYPE: {
                return "INTEGER8";
            }
            case SHORT_TYPE: {
                return "INTEGER2";
            }
            case STRING_TYPE: {
                return "UTF8";
            }
            case STRUCT_TYPE: {
                String definition = "RECORD\n";
                for (SparkField childField : field.children) {
                    definition += "\t" + getEClTypeDefinition(childField,recordDefinitionMap) + " " + childField.name + ";\n";
                } 
                definition += "END;\n";

                int hash = definition.hashCode();
                String recordDefnName = "##" + hash + "##";

                recordDefinitionMap.put(recordDefnName,definition);
                return recordDefnName;
            }
            default: {
                throw new Exception("Unable to generate ECL unknown field type: " + field.type);
            }
        }
    }
}