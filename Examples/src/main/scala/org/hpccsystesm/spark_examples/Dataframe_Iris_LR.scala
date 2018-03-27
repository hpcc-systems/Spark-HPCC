package main.scala.org.hpccsystesm.spark_examples
import org.hpccsystems.spark.HpccFile
import org.hpccsystems.spark.thor.RemapInfo
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.io.StdIn

object Dataframe_Iris_LR {
  def main(args: Array[String]) {
    val hpcc_jar = StdIn.readLine("Full path of Spark-HPCC Jar: ")
    val japi_jar = StdIn.readLine("Full path of JAPI Jar: ")
    val jar_list = Array(hpcc_jar, japi_jar)
    // Spark setup
    val conf = new SparkConf().setAppName("Iris_Spark_HPCC")
    conf.setMaster("local[2]")
    val sparkHome = StdIn.readLine("Full path to Spark home: ")
    conf.setSparkHome(sparkHome)
    conf.setJars(jar_list)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val hpcc_protocol = StdIn.readLine("protocol: ")
    val hpcc_ip = StdIn.readLine("ESP IP: ")
    val hpcc_port = StdIn.readLine("port: ")
    val hpcc_file = StdIn.readLine("File name: ")
    val user = StdIn.readLine("user: ")
    val pword = StdIn.readLine("password: ")
    val nodes = StdIn.readLine("nodes: ")
    val base_ip = StdIn.readLine("base ip: ")
    val hpcc = if (nodes.equals("") || base_ip.equals(""))
      new HpccFile(hpcc_file, hpcc_protocol, hpcc_ip, hpcc_port, user, pword)
    else {val ri = new RemapInfo(Integer.parseInt(nodes), base_ip)
      new HpccFile(hpcc_file, hpcc_protocol, hpcc_ip, hpcc_port, user, pword, ri)
    }
    val my_df = hpcc.getDataframe(spark)
    val assembler = new VectorAssembler()
    assembler.setInputCols(Array("petal_length","petal_width", "sepal_length", "sepal_width"))
    assembler.setOutputCol("features")
    val iris_fv = assembler.transform(my_df).withColumnRenamed("class", "label")
    val lr = new LogisticRegression()
    val iris_model = lr.fit(iris_fv)
    val with_preds = iris_model.transform(iris_fv)
    val predictionAndLabel = with_preds.rdd.map(
                      r => (r.getDouble(r.fieldIndex("prediction")),
                            r.getDouble(r.fieldIndex("label"))))
    val metrics = new MulticlassMetrics(predictionAndLabel)
    println("Confusion matrix:")
    println(metrics.confusionMatrix)
  }
}