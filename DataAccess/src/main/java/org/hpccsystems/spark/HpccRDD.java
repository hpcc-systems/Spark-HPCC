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

import org.apache.spark.Dependency;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;


/**
 * The implementation of the RDD<Record> (an RDD of type Record data) class.
 *
 */
public class HpccRDD extends RDD<Record> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final ClassTag<Record> CT_RECORD
                          = ClassTag$.MODULE$.apply(Record.class);
  private static Seq<Dependency<RDD<Record>>> empty
          = new ArraySeq<Dependency<RDD<Record>>>(0);
  //
  private HpccPart[] parts;
  private RecordDef def;
  /**
   * @param _sc
   * @param
   */
  public HpccRDD(SparkContext _sc, HpccPart[] parts, RecordDef def) {
    super(_sc, (Seq)empty, CT_RECORD);
    this.parts = new HpccPart[parts.length];
    for (int i=0; i<parts.length; i++) {
      this.parts[i] = parts[i];
    }
    this.def = def;
  }
  /**
   * Wrap this RDD as a JavaRDD so the Java API can be used.
   * @return a JavaRDD wrapper of the HpccRDD.
   */
  public JavaRDD<Record> asJavaRDD() {
    JavaRDD<Record> jRDD = new JavaRDD<Record>(this, CT_RECORD);
    return jRDD;
  }
  /**
   * Transform to an RDD of labeled points for MLLib supervised learning.
   * @param labelName the field name of the label datg
   * @param dimNames the field names for the dimensions
   * @return
   */
  public RDD<LabeledPoint> makeMLLibLabeledPoint(String labelName, String[] dimNames) {
    JavaRDD<Record> jRDD = this.asJavaRDD();
    Function<Record, LabeledPoint> map_f = new Function<Record, LabeledPoint>() {
      static private final long serialVersionUID = 1L;
      public LabeledPoint call(Record r) {
        return r.asLabeledPoint(labelName, dimNames);
      }
    };
    return jRDD.map(map_f).rdd();
  }
  /**
   * Transform to mllib.linalg.Vectors for ML Lib machine learning.
   * @param dimNames the field names for the dimensions
   * @return
   */
  public RDD<Vector> makeMLLibVector(String[] dimNames) {
    JavaRDD<Record> jRDD = this.asJavaRDD();
    Function<Record, Vector> map_f = new Function<Record, Vector>() {
      static private final long serialVersionUID = 1L;
      public Vector call(Record r) {
        return r.asMlLibVector(dimNames);
      }
    };
    return jRDD.map(map_f).rdd();
  }

  /* (non-Javadoc)
   * @see org.apache.spark.rdd.RDD#compute(org.apache.spark.Partition, org.apache.spark.TaskContext)
   */
  @Override
  public InterruptibleIterator<Record> compute(Partition p_arg, TaskContext ctx) {
    final HpccPart this_part = (HpccPart) p_arg;
    final RecordDef rd = this.def;
    Iterator<Record> iter = new Iterator<Record>() {
      private HpccRemoteFileReader rfr = new HpccRemoteFileReader(this_part, rd);
      //
      public boolean hasNext() { return this.rfr.hasNext();}
      public Record next() { return this.rfr.next(); }
    };
    scala.collection.Iterator<Record> s_iter
        = JavaConverters.asScalaIteratorConverter(iter).asScala();
    InterruptibleIterator<Record> rslt
        = new InterruptibleIterator<Record>(ctx, s_iter);
    return rslt;
  }

  /* (non-Javadoc)
   * @see org.apache.spark.rdd.RDD#getPartitions()
   */
  @Override
  public Partition[] getPartitions() {
    HpccPart[] rslt = new HpccPart[this.parts.length];
    for (int i=0; i<this.parts.length; i++) rslt[i] = this.parts[i];
    return rslt;
  }

}
