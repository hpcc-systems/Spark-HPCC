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

import org.apache.spark.sql.Row;
import org.hpccsystems.commons.ecl.RecordDef;
import org.hpccsystems.commons.errors.HpccFileException;
import org.hpccsystems.dafilesrv.client.BinaryRecordReader;
import org.hpccsystems.dafilesrv.client.DataPartition;

public class BinaryRecordReader4Spark extends BinaryRecordReader
{
	public BinaryRecordReader4Spark(DataPartition dp, RecordDef rd)
	{
		super(dp, rd);
	}
/*
	@Override
	public Object getNext() throws HpccFileException
	{
	    return super.getNext();
	}
*/
    public Row getNextRow() throws HpccFileException
    {
	    Object resultObj = super.getNext();

	    //logic to transform resultObj to Row needed!!!
	    return (Row) resultObj;
	  }
}
