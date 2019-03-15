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
import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import scala.reflect.ClassTag$;

import org.hpccsystems.commons.errors.HpccFileException;
import org.hpccsystems.dfs.client.CompressionAlgorithm;
import org.hpccsystems.dfs.client.DataPartition;
import org.hpccsystems.dfs.client.HPCCRemoteFileWriter;
import org.hpccsystems.dfs.cluster.RemapInfo;
import org.hpccsystems.dfs.cluster.NullRemapper;
import org.hpccsystems.commons.ecl.FieldDef;
import org.hpccsystems.commons.ecl.RecordDefinitionTranslator;

import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.utils.Connection;
import org.hpccsystems.ws.client.wrappers.wsdfu.DFUCreateFileWrapper;
import org.hpccsystems.ws.client.wrappers.wsdfu.DFUFilePartWrapper;
import org.hpccsystems.spark.SparkSchemaTranslator;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.execution.python.EvaluatePython;

public class HpccFileWriter implements Serializable {
    static private final long serialVersionUID = 1L;
    static private final int DefaultExpiryTimeSecs = 300;
    static private final Logger log = Logger.getLogger(HpccFileWriter.class.getName());

    // Transient so Java serialization does not try to serialize this
    private transient HPCCWsDFUClient dfuClient = null;

    // Make sure Python picklers have been registered
    static {
        EvaluatePython.registerPicklers();
    }

    public HpccFileWriter(Connection espconninfo) throws HpccFileException
    {
        this.dfuClient = HPCCWsDFUClient.get(espconninfo);
    }

    /**
    * HpccFileWriter Constructor
    * Attempts to open a connection to the specified HPCC cluster and validates the user.
    * @param connectionString of format {http|https}://{HOST}:{PORT}. Host & port are the same as the ecl watch host & port.
    * @param user a valid ecl watch account
    * @param pass the password for the provided user
    * @throws Exception 
    */
    public HpccFileWriter(String connectionString, String user, String pass) throws Exception {
        // Verify connection & password
        final Pattern connectionRegex = Pattern.compile("(http|https)://([^:]+):([0-9]+)",Pattern.CASE_INSENSITIVE);
        Matcher matches = connectionRegex.matcher(connectionString);
        if (matches.find() == false) {
            throw new Exception("Invalid connection string. Expected format: {http|https}://{HOST}:{PORT}");
        }

        Connection conn = new Connection(matches.group(1), matches.group(2), matches.group(3));
        conn.setUserName(user);
        conn.setPassword(pass);
        this.dfuClient = HPCCWsDFUClient.get(conn);
    }

    private void abortFileCreation() {
        log.error("Abort file creation was called. This is currently a stub.");
    }

    private class FilePartWriteResults implements Serializable {
        static private final long serialVersionUID = 1L;

        public long numRecords = 0;
        public long dataLength = 0;
        public boolean successful = true; // Default to true for empty partitions
        public String errorMessage = null;
    }
    
    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(RDD<Row> scalaRDD, String clusterName, String fileName) throws Exception {
        return this.saveToHPCC(SparkContext.getOrCreate(),scalaRDD,clusterName,fileName,CompressionAlgorithm.DEFAULT,false);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @param fileCompression compression algorithm to use on files
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(RDD<Row> scalaRDD, String clusterName, String fileName, CompressionAlgorithm fileCompression, boolean overwrite) throws Exception {
        return this.saveToHPCC(SparkContext.getOrCreate(),scalaRDD,clusterName,fileName,fileCompression,overwrite);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * @param sc The current SparkContext
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(SparkContext sc, RDD<Row> scalaRDD, String clusterName, String fileName) throws Exception {
        return saveToHPCC(sc,scalaRDD,clusterName,fileName,CompressionAlgorithm.NONE,false);
    }
    
    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * @param sc The current SparkContext
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @param fileCompression compression algorithm to use on files
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(SparkContext sc, RDD<Row> scalaRDD, String clusterName, String fileName, CompressionAlgorithm fileCompression, boolean overwrite) throws Exception {
        
        // Cache the RDD so we get the same partition mapping
        JavaRDD<Row> rdd = JavaRDD.fromRDD(scalaRDD, ClassTag$.MODULE$.apply(Row.class));

        //------------------------------------------------------------------------------
        //  Request a temp file be created in HPCC to write to
        //------------------------------------------------------------------------------

        Row firstRow = scalaRDD.first();
        StructType schema = firstRow.schema();
        FieldDef recordDef  = SparkSchemaTranslator.toHPCCRecordDef(schema);
        String eclRecordDefn = RecordDefinitionTranslator.toECLRecord(recordDef);
        DFUCreateFileWrapper createResult = dfuClient.createFile(fileName,clusterName,eclRecordDefn,DefaultExpiryTimeSecs);

        DFUFilePartWrapper[] dfuFileParts = createResult.getFileParts();
        DataPartition[] hpccPartitions = DataPartition.createPartitions(dfuFileParts, new NullRemapper(new RemapInfo(),createResult.getFileAccessInfo()), dfuFileParts.length, createResult.getFileAccessInfoBlob());

        if (hpccPartitions.length != rdd.getNumPartitions()) {
            rdd.repartition(hpccPartitions.length);
            if (rdd.getNumPartitions() != hpccPartitions.length) {
                throw new Exception("Repartitioning RDD failed. Aborting write.");
            }
        }


        /*
        int filePartsPerPartition = dfuFileParts.length / numPartitions;

        int residualFileParts = dfuFileParts.length % numPartitions;
        int residualFilePartsModulo = -1;

        // residualPartitionModulo = ceil(numPartitions / residualFileParts);
        // Doing the following to use only integer math to avoid potential floating point issues
        if (residualFileParts > 0) {
            int roundUpFactor = (residualFileParts + 1) / 2;
            residualFilePartsModulo = (numPartitions + roundUpFactor) / residualFileParts;
        }
        */

        //------------------------------------------------------------------------------
        //  Write partitions to file parts
        //------------------------------------------------------------------------------

        Function2<Integer, Iterator<Row>, Iterator<FilePartWriteResults>> writeFunc = 
        (Integer partitionIndex, Iterator<Row> it) -> {
            GenericRowRecordAccessor recordAccessor = new GenericRowRecordAccessor(recordDef);
            HPCCRemoteFileWriter<Row> fileWriter = new HPCCRemoteFileWriter<Row>(hpccPartitions[partitionIndex], recordDef, recordAccessor, fileCompression);

            FilePartWriteResults result = new FilePartWriteResults();
            try {
                fileWriter.writeRecords(it);
                fileWriter.close();
                
                result.dataLength = fileWriter.getBytesWritten();
                result.numRecords = fileWriter.getRecordsWritten();
                result.successful = true;
            } catch (Exception e) {
                result.successful = false;
                result.errorMessage = e.getMessage();
            }

            List<FilePartWriteResults> resultList = Arrays.asList(result);
            return resultList.iterator();
        };

        // Create Write Job
        JavaRDD<FilePartWriteResults> writeResultsRDD = rdd.mapPartitionsWithIndex(writeFunc,true);
        List<FilePartWriteResults> writeResultsList = writeResultsRDD.collect();

        long recordsWritten = 0;
        long dataWritten = 0;
        for (int i = 0; i < writeResultsList.size(); i++) {
            FilePartWriteResults result = writeResultsList.get(i);
            recordsWritten += result.numRecords;
            dataWritten += result.dataLength;

            if (result.successful == false) {
                abortFileCreation();
                throw new Exception("Writing failed with error: " + result.errorMessage);
            }
        }
        
        //------------------------------------------------------------------------------
        //  Publish and finalize the temp file
        //------------------------------------------------------------------------------

        try {
            dfuClient.publishFile(createResult.getFileID(),eclRecordDefn,recordsWritten,dataWritten,overwrite);
        } catch (Exception e) {
            throw new Exception("Failed to publish file with error: " + e.getMessage());
        }

        return recordsWritten;
    }

}
