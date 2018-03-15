package org.hpccsystems.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import java.io.Serializable;

/**
 * @author holtjd
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
