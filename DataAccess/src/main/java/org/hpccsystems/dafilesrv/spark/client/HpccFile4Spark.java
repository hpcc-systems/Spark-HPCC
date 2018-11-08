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

import java.net.MalformedURLException;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.python.EvaluatePython;
import org.hpccsystems.commons.cluster.RemapInfo;
import org.hpccsystems.commons.errors.HpccFileException;
import org.hpccsystems.dafilesrv.client.DataPartition;
import org.hpccsystems.dafilesrv.client.HPCCFile;
import org.hpccsystems.ws.client.utils.Connection;

public class HpccFile4Spark extends HPCCFile
{

	// Make sure Python picklers have been registered
	static { EvaluatePython.registerPicklers(); }

	/**
	 * 
	 */
	private static final long serialVersionUID = 3510030979404873361L;

	public HpccFile4Spark(String fileName, Connection espconninfo) throws HpccFileException {
		super(fileName, espconninfo);

	}

	/**
   * Constructor for the HpccFile.
   * Captures HPCC logical file  information from the DALI Server
   * for the clusters behind the ESP named by the Connection.
   *
   * @param fileName The HPCC file name
   * @param connectionString to eclwatch. Format: {http|https}://{HOST}:{PORT}.
   * @throws HpccFileException
   */
  public HpccFile4Spark(String fileName, String connectionString, String user, String pass) throws MalformedURLException, HpccFileException
  {
	  super(fileName, new Connection(connectionString, user, pass));
  }

  /**
   * Constructor for the HpccFile.
   * Captures HPCC logical file information from the DALI Server for the
   * clusters behind the ESP named by the IP address and re-maps
   * the address information for the THOR nodes to visible addresses
   * when the THOR clusters are virtual.
   * @param fileName The HPCC file name
   * @param targetColumnList a comma separated list of column names in dotted
   * notation for columns within compound columns.
   * @param filter a file filter to select records of interest
   * @param remap_info address and port re-mapping info for THOR cluster
   * @param maxParts optional the maximum number of partitions or zero for no max
   * @param targetfilecluster optional - the hpcc cluster the target file resides in
   * @throws HpccFileException
   */
  public HpccFile4Spark(String fileName, Connection espconninfo, String targetColumnList, String filter, RemapInfo remap_info, int maxParts, String targetfilecluster) throws HpccFileException
  {
	  super(fileName, espconninfo, targetColumnList, filter, remap_info, maxParts, targetColumnList);
  }

    /**
    * Make a Spark Resilient Distributed Dataset (RDD) that provides access
    * to THOR based datasets. Uses existing SparkContext, allows this function
    * to be used from PySpark.
    * @return An RDD of THOR data.
    * @throws HpccFileException When there are errors reaching the THOR data
    */
    public HpccRDD getRDD() throws HpccFileException
    {
        return getRDD(SparkContext.getOrCreate());
    }

    /**
    * Make a Spark Resilient Distributed Dataset (RDD) that provides access
    * to THOR based datasets.
    * @param sc Spark Context
    * @return An RDD of THOR data.
    * @throws HpccFileException When there are errors reaching the THOR data
    */
    public HpccRDD getRDD(SparkContext sc) throws HpccFileException
    {
	    return new HpccRDD(sc, getFileParts(), getRecordDefinition());
    }

    /**
    * Make a Spark Dataframe (Dataset<Row>) of THOR data available.
    * @param session the Spark Session object
    * @return a Dataframe of THOR data
    * @throws HpccFileException when htere are errors reaching the THOR data.
    */
    public Dataset<Row> getDataframe(SparkSession session) throws HpccFileException
    {
    	RecordDef4Spark rd = this.getRecordDefinition();
        DataPartition[] fp = this.getFileParts();
        JavaRDD<Row > rdd = (new HpccRDD(session.sparkContext(), fp, rd)).toJavaRDD();

        return session.createDataFrame(rdd, rd.asSchema());
    }
    
    public RecordDef4Spark getRecordDefinition() throws HpccFileException
    {
    	return (RecordDef4Spark)super.getRecordDefinition();
    }
}
