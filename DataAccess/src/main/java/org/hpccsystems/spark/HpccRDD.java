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
import java.util.Iterator;

import java.util.Arrays;

import org.apache.spark.Dependency;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.log4j.Logger;

import org.hpccsystems.dfs.client.DataPartition;
import org.hpccsystems.dfs.client.HpccRemoteFileReader;
import org.hpccsystems.commons.ecl.FieldDef;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * The implementation of the RDD<GenericRowWithSchema>
 *
 */
public class HpccRDD extends RDD<Row> implements Serializable
{
    private static final long          serialVersionUID = 1L;
    private static final Logger        log              = Logger.getLogger(HpccRDD.class.getName());
    private static final ClassTag<Row> CT_RECORD        = ClassTag$.MODULE$.apply(Row.class);

    private InternalPartition[]        parts;
    private FieldDef                   def;

    private class InternalPartition implements Partition
    {
        private static final long serialVersionUID = 1L;

        public DataPartition      partition;

        public int hashCode()
        {
            return this.index();
        }

        public int index()
        {
            return partition.index();
        }
    }

    /**
     * @param sc
     * @param dataParts
     * @param recordDefinition
    */
    public HpccRDD(SparkContext sc, DataPartition[] dataParts, FieldDef recordDefinition)
    {
        super(sc, new ArrayBuffer<Dependency<?>>(), CT_RECORD);
        this.parts = new InternalPartition[dataParts.length];
        for (int i = 0; i < dataParts.length; i++)
        {
            this.parts[i] = new InternalPartition();
            this.parts[i].partition = dataParts[i];
        }
        this.def = recordDefinition;
    }

    /**
     * Wrap this RDD as a JavaRDD so the Java API can be used.
     * @return a JavaRDD wrapper of the HpccRDD.
     */
    public JavaRDD<Row> asJavaRDD()
    {
        JavaRDD<Row> jRDD = new JavaRDD<Row>(this, CT_RECORD);
        return jRDD;
    }

    /**
     * Transform to an RDD of labeled points for MLLib supervised learning.
     * @param labelName the field name of the label datg
     * @param dimNames the field names for the dimensions
     * @throws IllegalArgumentException
     * @return
     */
    public RDD<LabeledPoint> makeMLLibLabeledPoint(String labelName, String[] dimNames) throws IllegalArgumentException
    {
        StructType schema = null;
        try
        {
            schema = SparkSchemaTranslator.toSparkSchema(this.def);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }

        // Precompute indices for the requested fields
        // Throws illegal argument exception if field cannot be found
        int labelIndex = schema.fieldIndex(labelName);
        int[] dimIndices = new int[dimNames.length];
        for (int i = 0; i < dimIndices.length; i++)
        {
            dimIndices[i] = schema.fieldIndex(dimNames[i]);
        }

        // Map each row to a labeled point using the precomputed indices
        JavaRDD<Row> jRDD = this.asJavaRDD();
        return jRDD.map((row) ->
        {
            double label = row.getDouble(labelIndex);
            double[] dims = new double[dimIndices.length];
            for (int i = 0; i < dimIndices.length; i++)
            {
                dims[i] = row.getDouble(dimIndices[i]);
            }
            return new LabeledPoint(label, new DenseVector(dims));
        }).rdd();
    }

    /**
     * Transform to mllib.linalg.Vectors for ML Lib machine learning.
     * @param dimNames the field names for the dimensions
     * @throws IllegalArgumentException
     * @return
     */
    public RDD<Vector> makeMLLibVector(String[] dimNames) throws IllegalArgumentException
    {
        StructType schema = null;
        try
        {
            schema = SparkSchemaTranslator.toSparkSchema(this.def);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException(e.getMessage());
        }

        // Precompute indices for the requested fields
        // Throws illegal argument exception if field cannot be found
        int[] dimIndices = new int[dimNames.length];
        for (int i = 0; i < dimIndices.length; i++)
        {
            dimIndices[i] = schema.fieldIndex(dimNames[i]);
        }

        // Map each row to a vector using the precomputed indices
        JavaRDD<Row> jRDD = this.asJavaRDD();
        return jRDD.map((row) ->
        {
            double[] dims = new double[dimIndices.length];
            for (int i = 0; i < dimIndices.length; i++)
            {
                dims[i] = row.getDouble(dimIndices[i]);
            }
            return (Vector) new DenseVector(dims);
        }).rdd();
    }

    /* (non-Javadoc)
     * @see org.apache.spark.rdd.RDD#compute(org.apache.spark.Partition, org.apache.spark.TaskContext)
     */
    @Override
    public InterruptibleIterator<Row> compute(Partition p_arg, TaskContext ctx)
    {
        final InternalPartition this_part = (InternalPartition) p_arg;
        final FieldDef rd = this.def;

        HpccRemoteFileReader<Row> fileReader = null;
        try
        {
            fileReader = new HpccRemoteFileReader<Row>(this_part.partition, rd, new GenericRowRecordBuilder(rd));
        }
        catch (Exception e)
        {
            log.error("Failed to create remote file reader with error: " + e.getMessage());
            return null;
        }

        scala.collection.Iterator<Row> iter = JavaConverters.asScalaIteratorConverter(fileReader).asScala();
        return new InterruptibleIterator<Row>(ctx, iter);
    }

    @Override
    public Seq<String> getPreferredLocations(Partition split)
    {
        final InternalPartition part = (InternalPartition) split;
        return JavaConverters.asScalaBufferConverter(Arrays.asList(part.partition.getCopyLocations()[0])).asScala().seq();
    }

    /* (non-Javadoc)
     * @see org.apache.spark.rdd.RDD#getPartitions()
     */
    @Override
    public Partition[] getPartitions()
    {
        return parts;
    }

}
