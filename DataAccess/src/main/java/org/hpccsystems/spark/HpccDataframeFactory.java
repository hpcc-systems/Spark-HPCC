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

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 */
public class HpccDataframeFactory implements Serializable{
  private final static long serialVersionUID = 1L;
  private SparkSession ss;    // the Spark Context equivalent for Dataframe API

  /**
   * The factory for creating Dataset<HpccRow> objects that provide
   * THOR datasets.
   */
  public HpccDataframeFactory(SparkSession session) {
    this.ss = session;
  }
  Dataset<Row> getDataframe(HpccFile hpcc) throws HpccFileException {
    RecordDef rd = hpcc.getRecordDefinition();
    FilePart[] fp = hpcc.getFileParts();
    JavaRDD<Record> jRDD = (new HpccRDD(ss.sparkContext(), fp, rd)).asJavaRDD();
    Function<Record, Row> map_f = new Function<Record, Row>() {
      static private final long serialVersionUID = 1L;
      public Row call(Record r) {
        return r.asRow(rd);
      }
    };
    JavaRDD<Row> rowRDD = jRDD.map(map_f);
    return ss.createDataFrame(rowRDD, rd.asSchema());
  }

}
