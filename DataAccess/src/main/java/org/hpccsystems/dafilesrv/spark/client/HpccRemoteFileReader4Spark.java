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
import org.hpccsystems.dafilesrv.client.DataPartition;
import org.hpccsystems.dafilesrv.client.HpccRemoteFileReader;

public class HpccRemoteFileReader4Spark extends HpccRemoteFileReader
{
	public HpccRemoteFileReader4Spark(DataPartition fp, RecordDef rd)
	{
		super(fp, rd);
	}

	/**
	 * Return next record
	 * @return the record
	 */
	public Row next()
	{
	    Row rslt = null;
	    try
	    {
	        rslt = (Row) brr.getNext();
	    }
	    catch (HpccFileException e)
	    {
	        System.err.println("Read failure for " + fp.toString());
	        e.printStackTrace(System.err);
	        throw new java.util.NoSuchElementException("Fatal read error");
	    }
	    return rslt;
	}
}
