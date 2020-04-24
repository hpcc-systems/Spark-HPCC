package org.hpccsystems.spark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.hpccsystems.commons.ecl.FieldDef;
import org.hpccsystems.dfs.client.DataPartition;
import org.hpccsystems.dfs.cluster.RemapInfo;
import org.hpccsystems.ws.client.utils.Connection;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import org.junit.Ignore;

/**
 * Test from to test RDD by reading the data and writing it to the console.
 *
 * Ignore for test suite, run manually
 *
 */

@Ignore
public class InteractiveRDDTest
{
    public static void main(String[] args) throws Exception
    {
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
        Seq<String> jar_seq = JavaConverters.iterableAsScalaIterableConverter(jar_list).asScala().toSeq();
        conf.setJars(jar_seq);
        System.out.println("Spark configuration set");
        SparkContext sc = new SparkContext(conf);
        System.out.println("Spark context available");
        System.out.println("Now need HPCC file information");
        System.out.print("Enter EclWatch protocol: ");
        System.out.flush();
        String protocol = br.readLine();
        System.out.print("Enter EclWatch ip: ");
        System.out.flush();
        String esp_ip = br.readLine();
        System.out.print("Enter EclWatch port: ");
        System.out.flush();
        String port = br.readLine();

        Connection espcon = new Connection(protocol, esp_ip, port);

        System.out.print("Enter HPCC file name: ");
        System.out.flush();
        String testName = br.readLine();
        System.out.print("Enter HPCC file cluster name(mythor,etc.): ");
        System.out.flush();
        String fileclustername = br.readLine();
        System.out.print("Enter EclWatch User ID: ");
        System.out.flush();

        espcon.setUserName(br.readLine());

        System.out.print("Enter EclWatch Password: ");
        System.out.flush();

        espcon.setPassword(br.readLine());

        HpccFile hpccFile = new HpccFile(testName, espcon);

        if (fileclustername.length() != 0) hpccFile.setTargetfilecluster(fileclustername);

        System.out.print("Enter Project Field list or empty: ");
        System.out.flush();
        String fieldList = br.readLine();
        if (fieldList.length() != 0) hpccFile.setProjectList(fieldList);

        System.out.print("Enter Record filter expression (or empty): ");
        System.out.flush();
        String filterExpression = br.readLine();

        if (filterExpression.length() != 0) hpccFile.setFilter(filterExpression);

        System.out.print("Number of nodes for remap (or empty): ");
        System.out.flush();
        String nodes = br.readLine();
        String base_ip = "";
        if (nodes.length() != 0)
        {
            System.out.print("Base IP: ");
            System.out.flush();
            base_ip = br.readLine();
            hpccFile.setClusterRemapInfo(new RemapInfo(Integer.parseInt(nodes), base_ip));
        }

        System.out.println("Getting file parts");
        DataPartition[] parts = hpccFile.getFileParts();
        System.out.println("Getting record definition");
        FieldDef rd = hpccFile.getRecordDefinition();
        System.out.println(rd.toString());
        System.out.println("Creating RDD");
        HpccRDD myRDD = new HpccRDD(sc, parts, rd);
        System.out.println("Getting local iterator");
        scala.collection.Iterator<Row> rec_iter = myRDD.toLocalIterator();

        while (rec_iter.hasNext())
        {
            Row rec = rec_iter.next();
            System.out.println(rec.toString());
        }

        System.out.println("Completed output of Record data");
        System.out.println("End of run");
    }
}
