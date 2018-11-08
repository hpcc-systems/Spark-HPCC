/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

package org.hpccsystems.dafilesrv.spark.client;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hpccsystems.commons.ecl.FieldDef;

public class FieldDef4Spark extends FieldDef 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private StructType schema = null;
	private StructField schemaElement = null;

	  /**
	   * Translates a FieldDef into a StructType schema
	   * @return StructType
	   */
	  public StructType asSchema() 
	  {
	    if (getFieldType() != org.hpccsystems.commons.ecl.FieldType.RECORD)
	      return null;

	    if (this.schema != null)
	      return this.schema;


	    StructField[] fields = new StructField[this.getNumDefs()];
	    for (int i=0; i<this.getNumDefs(); i++)
	    {
	      fields[i] = ((FieldDef4Spark)getDef(i)).asSchemaElement();
	    }
	    this.schema = DataTypes.createStructType(fields);
	    return this.schema;
	  }
	  /**
	   * translate a FieldDef into a StructField object of the schema
	   * @return
	   */
	  public StructField asSchemaElement()
	  {
	    if (this.schemaElement != null) {
	      return this.schemaElement;
	    }

	    Metadata empty = Metadata.empty();
	    boolean nullable = false;

	    DataType type = DataTypes.NullType;
	    switch (getFieldType()) {
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
	        StructField childField = ((FieldDef4Spark)getDef(0)).asSchemaElement();
	        type = DataTypes.createArrayType(childField.dataType());
	        nullable = true;
	        break;
	      case RECORD:
	        StructField[] childFields = new StructField[getNumDefs()];
	        for (int i=0; i<getNumDefs(); i++)
	        {
	          childFields[i] = ((FieldDef4Spark) getDef(i)).asSchemaElement();
	        }
	        type = DataTypes.createStructType(childFields);
	        break;
	      case UNKNOWN:
	        type = DataTypes.NullType;
	        break;
	    }

	    this.schemaElement = new StructField(getFieldName(), type, nullable, empty);
	    return this.schemaElement; 
	  }
}
