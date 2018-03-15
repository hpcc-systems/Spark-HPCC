package org.hpccsystesm.spark_examples

import org.hpccsystems.spark.HpccFile
import org.hpccsystems.spark.HpccRDD
import org.hpccsystems.spark.thor.RemapInfo
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.StdIn

object Iris_LR {
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
    val sc = new SparkContext(conf)
    val hpcc_protocol = StdIn.readLine("protocol: ")
    val hpcc_ip = StdIn.readLine("ESP IP: ")
    val hpcc_port = StdIn.readLine("port: ")
    val hpcc_file = StdIn.readLine("File name: ")
    val user = StdIn.readLine("user: ")
    val pword = StdIn.readLine("password: ")
    val nodes = StdIn.readLine("nodes: ")
    val base_ip = StdIn.readLine("base ip: ")
    //
    //val ri = new RemapInfo(20, "10.240.37.108")
    //val hpcc = new HpccFile(hpcc_file, hpcc_protocol, hpcc_ip, hpcc_port, "", "", ri)
    val hpcc = if (nodes.equals("") || base_ip.equals(""))
      new HpccFile(hpcc_file, hpcc_protocol, hpcc_ip, hpcc_port, user, pword)
    else {val ri = new RemapInfo(Integer.parseInt(nodes), base_ip)
      new HpccFile(hpcc_file, hpcc_protocol, hpcc_ip, hpcc_port, user, pword, ri)
    }
    val myRDD = hpcc.getRDD(sc)
    val names = new Array[String](4)
    names(0) = "petal_length"
    names(1) = "petal_width"
    names(2) = "sepal_length"
    names(3) = "sepal_width"
    val lpRDD = myRDD.makeMLLibLabeledPoint("class", names)
    val lr = new LogisticRegressionWithLBFGS().setNumClasses(3)
    val iris_model = lr.run(lpRDD)
    val predictionAndLabel = lpRDD.map {case LabeledPoint(label, features) =>
      val prediction = iris_model.predict(features)
      (prediction, label)
    }
    val metrics = new MulticlassMetrics(predictionAndLabel)
    println("Confusion matrix:")
    println(metrics.confusionMatrix)
  }
}