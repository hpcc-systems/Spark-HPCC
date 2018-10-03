package org.hpccsystems.spark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

//import scala.collection.mutable.ArraySeq;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hpccsystems.spark.thor.DataPartition;
import org.hpccsystems.spark.thor.RemapInfo;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class DataframeTest {

  public DataframeTest() {
  }

  public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    SparkConf conf = new SparkConf().setAppName("Spark HPCC test");
    conf.setMaster("local[2]");
    System.out.print("Full path name to Spark: ");
    System.out.flush();
    String sparkHome = br.readLine();
    conf.setSparkHome(sparkHome);
    System.out.print("Full path to JAPI jar: ");
    System.out.flush();
    String japi_jar = br.readLine();
    System.out.print("Full path to Spark-HPCC jar: ");
    System.out.flush();
    String this_jar = br.readLine();
    // now have Spark inputs
    java.util.List<String> jar_list = Arrays.asList(this_jar, japi_jar);
    Seq<String> jar_seq = JavaConverters.iterableAsScalaIterableConverter(jar_list).asScala().toSeq();;
    conf.setJars(jar_seq);
    System.out.println("Spark configuration set");
    SparkSession session = SparkSession.builder().config(conf).getOrCreate();
    System.out.println("Spark session available");
    System.out.println("Now need HPCC file information");
    System.out.print("Enter protocol: ");
    System.out.flush();
    String protocol = br.readLine();
    System.out.print("Enter ip: ");
    System.out.flush();
    String esp_ip = br.readLine();
    System.out.print("Enter port: ");
    System.out.flush();
    String port = br.readLine();
    System.out.print("Enter file name: ");
    System.out.flush();
    String testName = br.readLine();
    System.out.print("User id: ");
    System.out.flush();
    String user = br.readLine();
    System.out.print("pass word: ");
    System.out.flush();
    String pword = br.readLine();
    System.out.print("Field list or empty: ");
    System.out.flush();
    String fieldList = br.readLine();
    System.out.print("Number of nodes for remap or empty: ");
    System.out.flush();
    String nodes = br.readLine();
    System.out.print("Base IP or empty: ");
    System.out.flush();
    String base_ip = br.readLine();
    HpccFile hpcc;
    if (nodes.equals("") || base_ip.equals("")) {
      hpcc = new HpccFile(testName, protocol, esp_ip, port, user, pword, fieldList, 0);
    } else {
      RemapInfo ri = new RemapInfo(Integer.parseInt(nodes), base_ip);
      hpcc = new HpccFile(testName, protocol, esp_ip, port, user, pword, fieldList, ri, 0);
    }
    System.out.println("Getting file parts");
    DataPartition[] parts = hpcc.getFileParts();
    for (DataPartition p : parts) System.out.println(p.toString());
    System.out.println("Getting record definition");
    RecordDef rd = hpcc.getRecordDefinition();
    System.out.println(rd.toString());
    System.out.println("Getting Dataframe");
    Dataset<Row> my_df = hpcc.getDataframe(session);
    my_df.show(150);
  }
}
