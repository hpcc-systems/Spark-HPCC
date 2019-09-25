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
import java.util.Map;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

import java.math.BigDecimal;

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
import org.hpccsystems.ws.client.wrappers.wsdfu.DFUFileTypeWrapper;
import org.hpccsystems.spark.SparkSchemaTranslator;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.execution.python.EvaluatePython;

import net.razorvine.pickle.Unpickler;

public class HpccFileWriter implements Serializable
{
    static private final long         serialVersionUID      = 1L;
    static private final int          DefaultExpiryTimeSecs = 300;
    static private final Logger       log                   = Logger.getLogger(HpccFileWriter.class.getName());

    // Transient so Java serialization does not try to serialize this
    private transient HPCCWsDFUClient dfuClient             = null;
    private transient Connection connectionInfo             = null;

    private static void registerPicklingFunctions()
    {
        EvaluatePython.registerPicklers();
        Unpickler.registerConstructor("pyspark.sql.types", "Row", new RowConstructor());
        Unpickler.registerConstructor("pyspark.sql.types", "_create_row", new RowConstructor());
        Unpickler.registerConstructor("org.hpccsystems", "PySparkField", new PySparkFieldConstructor());
    }

    static 
    {
        registerPicklingFunctions();
    }

    public HpccFileWriter(Connection espconninfo) throws HpccFileException
    {
        this.connectionInfo = espconninfo;
    }

    /**
    * HpccFileWriter Constructor
    * Attempts to open a connection to the specified HPCC cluster and validates the user.
    * @param connectionString of format {http|https}://{HOST}:{PORT}. Host & port are the same as the ecl watch host & port.
    * @param user a valid ecl watch account
    * @param pass the password for the provided user
    * @throws Exception 
    */
    public HpccFileWriter(String connectionString, String user, String pass) throws Exception
    {
        // Verify connection & password
        final Pattern connectionRegex = Pattern.compile("(http|https)://([^:]+):([0-9]+)", Pattern.CASE_INSENSITIVE);
        Matcher matches = connectionRegex.matcher(connectionString);
        if (matches.find() == false)
        {
            throw new Exception("Invalid connection string. Expected format: {http|https}://{HOST}:{PORT}");
        }

        this.connectionInfo = new Connection(matches.group(1), matches.group(2), matches.group(3));
        this.connectionInfo.setUserName(user);
        this.connectionInfo.setPassword(pass);
    }

    private void abortFileCreation()
    {
        log.error("Abort file creation was called. This is currently a stub.");
    }

    private class FilePartWriteResults implements Serializable
    {
        static private final long serialVersionUID = 1L;

        public long               numRecords       = 0;
        public long               dataLength       = 0;
        public boolean            successful       = true; // Default to true for empty partitions
        public String             errorMessage     = null;
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster. Will use HPCC default file compression.
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(RDD<Row> scalaRDD, String clusterName, String fileName) throws Exception
    {
        JavaRDD<Row> rdd = JavaRDD.fromRDD(scalaRDD, ClassTag$.MODULE$.apply(Row.class));
        return this.saveToHPCC(rdd, clusterName, fileName);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster. Will use HPCC default file compression.
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param schema The Schema of the provided RDD
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(StructType schema, RDD<Row> scalaRDD, String clusterName, String fileName) throws Exception
    {
        JavaRDD<Row> rdd = JavaRDD.fromRDD(scalaRDD, ClassTag$.MODULE$.apply(Row.class));
        return this.saveToHPCC(schema, rdd, clusterName, fileName);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster. Will use HPCC default file compression. 
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param javaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(JavaRDD<Row> javaRDD, String clusterName, String fileName) throws Exception
    {
        return this.saveToHPCC(SparkContext.getOrCreate(), null, javaRDD, clusterName, fileName, CompressionAlgorithm.DEFAULT, false);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster. Will use HPCC default file compression. 
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param schema The Schema of the provided RDD
    * @param javaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(StructType schema, JavaRDD<Row> javaRDD, String clusterName, String fileName) throws Exception
    {
        return this.saveToHPCC(SparkContext.getOrCreate(), schema, javaRDD, clusterName, fileName, CompressionAlgorithm.DEFAULT, false);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @param fileCompression compression algorithm to use on files
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(RDD<Row> scalaRDD, String clusterName, String fileName, CompressionAlgorithm fileCompression, boolean overwrite)
            throws Exception
    {
        JavaRDD<Row> rdd = JavaRDD.fromRDD(scalaRDD, ClassTag$.MODULE$.apply(Row.class));
        return this.saveToHPCC(SparkContext.getOrCreate(), null, rdd, clusterName, fileName, fileCompression, overwrite);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param schema The Schema of the provided RDD
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @param fileCompression compression algorithm to use on files
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(StructType schema, RDD<Row> scalaRDD, String clusterName, String fileName, CompressionAlgorithm fileCompression, boolean overwrite)
            throws Exception
    {
        JavaRDD<Row> rdd = JavaRDD.fromRDD(scalaRDD, ClassTag$.MODULE$.apply(Row.class));
        return this.saveToHPCC(SparkContext.getOrCreate(), schema, rdd, clusterName, fileName, fileCompression, overwrite);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param javaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @param fileCompression compression algorithm to use on files
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(JavaRDD<Row> javaRDD, String clusterName, String fileName, CompressionAlgorithm fileCompression, boolean overwrite)
            throws Exception
    {
        return this.saveToHPCC(SparkContext.getOrCreate(), null, javaRDD, clusterName, fileName, fileCompression, overwrite);
    }
    
    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param schema The Schema of the provided RDD
    * @param javaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @param fileCompression compression algorithm to use on files
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(StructType schema, JavaRDD<Row> javaRDD, String clusterName, String fileName, CompressionAlgorithm fileCompression, boolean overwrite)
            throws Exception
    {
        return this.saveToHPCC(SparkContext.getOrCreate(), schema, javaRDD, clusterName, fileName, fileCompression, overwrite);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster. Will use HPCC default file compression.
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param sc The current SparkContext
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(SparkContext sc, RDD<Row> scalaRDD, String clusterName, String fileName) throws Exception
    {
        JavaRDD<Row> rdd = JavaRDD.fromRDD(scalaRDD, ClassTag$.MODULE$.apply(Row.class));
        return saveToHPCC(sc, null, rdd, clusterName, fileName, CompressionAlgorithm.DEFAULT, false);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster. Will use HPCC default file compression.
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param sc The current SparkContext
    * @param javaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(SparkContext sc, JavaRDD<Row> javaRDD, String clusterName, String fileName) throws Exception
    {
        return saveToHPCC(sc, null, javaRDD, clusterName, fileName, CompressionAlgorithm.DEFAULT, false);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param sc The current SparkContext
    * @param RDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @param fileCompression compression algorithm to use on files
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(SparkContext sc, RDD<Row> scalaRDD, String clusterName, String fileName, CompressionAlgorithm fileCompression,
            boolean overwrite) throws Exception
    {
        JavaRDD<Row> rdd = JavaRDD.fromRDD(scalaRDD, ClassTag$.MODULE$.apply(Row.class));
        return this.saveToHPCC(sc, null, rdd, clusterName, fileName, fileCompression, overwrite);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * Note: PySpark datasets can be written to HPCC by first calling inferSchema to generate a valid Java Schema
    * and converting the PySpark RDD to a JavaRDD via the _py2java() helper
    * @param sc The current SparkContext
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @param fileCompression compression algorithm to use on files
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(SparkContext sc, StructType rddSchema, JavaRDD<Row> rdd, String clusterName, String fileName, CompressionAlgorithm fileCompression,
            boolean overwrite) throws Exception
    {
        this.dfuClient = HPCCWsDFUClient.get(this.connectionInfo);

        if (sc == null || rdd == null)
        {
            throw new Exception("Aborting write. A valid non-null SparkContext and RDD must be provided.");
        }

        StructType schema = rddSchema;
        if (schema == null)
        {
            Row firstRow = rdd.first();
            schema = firstRow.schema();
        }

        FieldDef recordDef = SparkSchemaTranslator.toHPCCRecordDef(schema);
        String eclRecordDefn = RecordDefinitionTranslator.toECLRecord(recordDef);
        boolean isCompressed = fileCompression != CompressionAlgorithm.NONE;
        DFUCreateFileWrapper createResult = dfuClient.createFile(fileName, clusterName, eclRecordDefn, DefaultExpiryTimeSecs, isCompressed, DFUFileTypeWrapper.Flat, null);

        DFUFilePartWrapper[] dfuFileParts = createResult.getFileParts();
        DataPartition[] hpccPartitions = DataPartition.createPartitions(dfuFileParts,
                new NullRemapper(new RemapInfo(), createResult.getFileAccessInfo()), dfuFileParts.length, createResult.getFileAccessInfoBlob());

        if (hpccPartitions.length != rdd.getNumPartitions())
        {
            rdd = rdd.repartition(hpccPartitions.length);
            if (rdd.getNumPartitions() != hpccPartitions.length)
            {
                throw new Exception("Repartitioning RDD failed. Aborting write.");
            }
        }

        //------------------------------------------------------------------------------
        //  Write partitions to file parts
        //------------------------------------------------------------------------------

        Function2<Integer, Iterator<Row>, Iterator<FilePartWriteResults>> writeFunc = (Integer partitionIndex, Iterator<Row> it) ->
        {
            HpccFileWriter.registerPicklingFunctions();
            GenericRowRecordAccessor recordAccessor = new GenericRowRecordAccessor(recordDef);
            HPCCRemoteFileWriter<Row> fileWriter = new HPCCRemoteFileWriter<Row>(hpccPartitions[partitionIndex], recordDef, recordAccessor,
                    fileCompression);
            FilePartWriteResults result = new FilePartWriteResults();
            try
            {
                fileWriter.writeRecords(it);
                fileWriter.close();

                result.dataLength = fileWriter.getBytesWritten();
                result.numRecords = fileWriter.getRecordsWritten();
                result.successful = true;
            }
            catch (Exception e)
            {
                result.successful = false;
                result.errorMessage = e.getMessage();
            }

            List<FilePartWriteResults> resultList = Arrays.asList(result);
            return resultList.iterator();
        };

        // Create Write Job
        JavaRDD<FilePartWriteResults> writeResultsRDD = rdd.mapPartitionsWithIndex(writeFunc, true);
        List<FilePartWriteResults> writeResultsList = writeResultsRDD.collect();

        long recordsWritten = 0;
        long dataWritten = 0;
        for (int i = 0; i < writeResultsList.size(); i++)
        {
            FilePartWriteResults result = writeResultsList.get(i);
            recordsWritten += result.numRecords;
            dataWritten += result.dataLength;

            if (result.successful == false)
            {
                abortFileCreation();
                throw new Exception("Writing failed with error: " + result.errorMessage);
            }
        }

        //------------------------------------------------------------------------------
        //  Publish and finalize the temp file
        //------------------------------------------------------------------------------

        try
        {
            dfuClient.publishFile(createResult.getFileID(), eclRecordDefn, recordsWritten, dataWritten, overwrite);
        }
        catch (Exception e)
        {
            throw new Exception("Failed to publish file with error: " + e.getMessage());
        }

        return recordsWritten;
    }

    /**
    * Generates an inferred schema based on an example Map of FieldNames -> Example Field Objects.
    * This function is targeted primary at helping PySpark users write datasets back to HPCC.
    * @param rowDictionary row dictionary used for inferrence
    * @return Returns a valid Spark schema based on the example rowDictionary
    * @throws Exception
    */
    public StructType inferSchema(List<PySparkField> exampleFields) throws Exception
    {
        return generateRowSchema(exampleFields);
    }

    private StructType generateRowSchema(List<PySparkField> exampleFields) throws Exception
    {
        StructField[] fields = new StructField[exampleFields.size()];

        int index = 0;
        for (PySparkField fieldInfo : exampleFields)
        {
            fields[index] = generateSchemaField(fieldInfo.getName(), fieldInfo.getValue());
            index++;
        }

        return new StructType(fields);
    }

    private StructField generateSchemaField(String name, Object obj) throws Exception
    {
        Metadata empty = Metadata.empty();
        boolean nullable = false;

        DataType type = DataTypes.NullType;
        if (obj instanceof String)
        {
            type = DataTypes.StringType;
        }
        else if (obj instanceof Byte)
        {
            type = DataTypes.ByteType;
        } 
        else if (obj instanceof Short)
        {
            type = DataTypes.ShortType;
        } 
        else if (obj instanceof Integer)
        {
            type = DataTypes.IntegerType;
        }
        else if (obj instanceof Long)
        {
            type = DataTypes.LongType;
        }
        else if (obj instanceof byte[])
        {
            type = DataTypes.BinaryType;
        }
        else if (obj instanceof Boolean)
        {
            type = DataTypes.BooleanType;
        }
        else if (obj instanceof Float)
        {
            type = DataTypes.FloatType;
        }
        else if (obj instanceof Double)
        {
            type = DataTypes.DoubleType;
        }
        else if (obj instanceof BigDecimal)
        {
            BigDecimal decimal = (BigDecimal) obj;
            int precision = decimal.precision();
            int scale = decimal.scale();

            // Spark SQL only supports 38 digits in decimal values
            if (precision > DecimalType.MAX_PRECISION()) 
            {
                scale -= (precision - DecimalType.MAX_PRECISION());
                if (scale < 0)
                {
                    scale = 0;
                }
                
                precision = DecimalType.MAX_PRECISION();
            }

            type = DataTypes.createDecimalType(precision,scale);
        }
        else if (obj instanceof List)
        {
            List<Object> list= (List<Object>) obj;
            if (list.size() == 0)
            {
                throw new Exception("Unable to infer row schema. Encountered an empty List: " + name 
                    + ". All lists must have an example row to infer schema.");
            }

            Object firstChild = list.get(0);
            if (firstChild instanceof PySparkField) 
            {
                List<PySparkField> rowFields = (List<PySparkField>)(List<?>) list;
                type = generateRowSchema(rowFields);
            }
            else
            {
                StructField childField = generateSchemaField("temp",firstChild);
                type = DataTypes.createArrayType(childField.dataType());
                nullable = true;
            }
        }
        else
        {
            throw new Exception("Encountered unsupported type: " + obj.getClass().getName() 
                + ". Ensure that the entire example row hierarchy has been converted to a Dictionary. Including rows in child datasets.");
        }

        return new StructField(name, type, nullable, empty);
    }

}
