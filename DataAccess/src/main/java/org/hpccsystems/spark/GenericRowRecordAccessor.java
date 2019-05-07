/*******************************************************************************
 *     HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.
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

import org.hpccsystems.dfs.client.IRecordAccessor;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import org.hpccsystems.commons.ecl.FieldDef;
import org.hpccsystems.commons.ecl.FieldType;

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;

public class GenericRowRecordAccessor implements IRecordAccessor
{
    private GenericRowWithSchema row                  = null;
    private FieldDef             fieldDef             = null;
    private GenericRowRecordAccessor[] childRecordAccessors = null;

    public GenericRowRecordAccessor(StructType schema) throws IllegalArgumentException
    {
        try
        {
            this.fieldDef = SparkSchemaTranslator.toHPCCRecordDef(schema);
        }
        catch(Exception e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public GenericRowRecordAccessor(FieldDef fieldDef)
    {
        this.fieldDef = fieldDef;
        this.childRecordAccessors = new GenericRowRecordAccessor[this.fieldDef.getNumDefs()];
        for (int i = 0; i < this.fieldDef.getNumDefs(); i++)
        {
            FieldDef fd = this.fieldDef.getDef(i);
            boolean needsChildRecordAccessor = (fd.getFieldType() == FieldType.RECORD)
                    || (fd.getFieldType() == FieldType.DATASET && fd.getDef(0).getFieldType() == FieldType.RECORD);

            if (needsChildRecordAccessor)
            {
                FieldDef subFd = fd;
                if (fd.getFieldType() == FieldType.DATASET)
                {
                    subFd = fd.getDef(0);
                }
                childRecordAccessors[i] = new GenericRowRecordAccessor(subFd);
            }
            else
            {
                childRecordAccessors[i] = null;
            }
        }

    }

    public IRecordAccessor setRecord(Object rd)
    {
        this.row = (GenericRowWithSchema) rd;
        return this;
    }

    public int getNumFields()
    {
        return this.fieldDef.getNumDefs();
    }

    public Object getFieldValue(int index)
    {
        Object value = this.row.get(index);
        if (value instanceof Seq)
        {
            Seq seqValue = (Seq) value;
            return JavaConverters.seqAsJavaListConverter(seqValue).asJava();
        }
        else
        {
            return value;
        }
    }

    public FieldDef getFieldDefinition(int index)
    {
        return this.fieldDef.getDef(index);
    }

    public IRecordAccessor getChildRecordAccessor(int index)
    {
        return this.childRecordAccessors[index];
    }
}
