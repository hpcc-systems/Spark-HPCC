package org.hpccsystems.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.hpccsystems.commons.ecl.FieldDef;
import org.hpccsystems.dfs.client.DataPartition;
import org.hpccsystems.ws.client.platform.test.BaseRemoteTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Junit tests for RDD.
 *
 */

@Category(org.hpccsystems.commons.annotations.RemoteTests.class)
public class RDDTest extends BaseRemoteTest
{
    protected SparkConf conf = new SparkConf().setAppName("Spark/HPCC JUnit test");
    protected SparkContext sc = null; //must be closed after every test
    protected HpccFile hpccFile = null;

    protected String sparkRuntimePath       = System.getProperty("sparkruntimepath");
    protected String japiRuntimePath        = System.getProperty("japiruntimepath");
    protected String sparkMaster            = System.getProperty("sparkmaster");
    protected String sparkhpccconnectorpath = System.getProperty("sparkhpccconnectorpath");
    protected String hpccfilename           = System.getProperty("hpccfilename");
    protected String hpccfilecluster        = System.getProperty("hpccfilecluster");
    protected String projectList            = System.getProperty("projectlist");
    protected String keyFilter              = System.getProperty("keyFilter");

    public static final String DEFAULTHPCCFILENAME = "benchmark::all_types::200kb";

    static
    {
        if (System.getProperty("hpccfilename") == null)
            System.out.println("hpccfilename not provided - defaulting to '" + DEFAULTHPCCFILENAME + "'");

        if (System.getProperty("hpccfilecluster") == null)
            System.out.println("hpccfilecluster not provided - defaulting to 'null'");

        if (System.getProperty("sparkRuntimePath") == null)
            System.out.println("sparkruntimepath not provided - defaulting to ''");

        if (System.getProperty("japiruntimepath") == null)
            System.out.println("japiruntimepath not provided - defaulting to ''");

        if (System.getProperty("sparkmaster") == null)
        {
            System.out.println("sparkmaster not provided - defaulting to 'local[2]'");
            System.out.println("The master URL to connect to, such as \"local\" to run locally with one thread, \"local[4]\" to run locally with 4 cores, or \"spark://master:7077\" to run on a Spark standalone cluster.");
        }

        if (System.getProperty("sparkhpccconnectorpath") == null)
            System.out.println("sparkhpccconnectorpath not provided - defaulting to ''");

        if (System.getProperty("projectlist") == null)
            System.out.println("projectlist not provided - defaulting to ''");

        if (System.getProperty("keyfilter") == null)
            System.out.println("keyfilter not provided - defaulting to ''");

        if (System.getProperty("keyfilter") == null)
            System.out.println("keyfilter not provided - defaulting to ''");
    }

    @Before
    public void setUp() throws Exception
    {
        Assert.assertNotNull("Could not setup SPARK configuration", conf);
        sparkMaster = System.getProperty("sparkmaster");

        if (sparkMaster == null || sparkMaster.isEmpty())
            sparkMaster = "local[*]";

        conf.setMaster(sparkMaster);

        if (sparkRuntimePath != null)
            conf.setSparkHome(sparkRuntimePath);

        // now have Spark inputs
        java.util.List<String> jar_list = Arrays.asList(sparkhpccconnectorpath, japiRuntimePath);
        Seq<String> runtimeJars = JavaConverters.iterableAsScalaIterableConverter(jar_list).asScala().toSeq();

        conf.setJars(runtimeJars);

        sc = new SparkContext(conf);
        Assert.assertNotNull("Could not setup SPARK context", sc);

        System.out.println("Spark context available");

        if (hpccfilename == null || hpccfilename.isEmpty())
            hpccfilename = DEFAULTHPCCFILENAME;

        hpccFile = new HpccFile(hpccfilename, connection);
        Assert.assertNotNull("Could not create hpccFile object");

        if (hpccfilecluster != null && !hpccfilecluster.isEmpty())
            hpccFile.setTargetfilecluster(hpccfilecluster);

        if (projectList != null && !projectList.isEmpty())
            hpccFile.setProjectList(projectList);
    }

    @After
    public void teardown()
    {
        if (sc != null)
            sc.stop();
    }

    @Test
    public void simpleRDDTest()
    {
        System.out.println("\n----------Spark HPCC RDD Test----------");

        try
        {
            try
            {
                if (keyFilter != null && !keyFilter.isEmpty())
                    hpccFile.setFilter(keyFilter);
            }
            catch (Exception e)
            {
                Assert.fail("Failed setting filter: " + e.getLocalizedMessage());
            }

            System.out.println("Getting file parts");
            DataPartition[] parts = hpccFile.getFileParts();
            System.out.println();

            FieldDef rd = hpccFile.getRecordDefinition();
            System.out.println("Original record definition: " + rd.toString() + "\n");
            System.out.println("Projected record definition: " + hpccFile.getProjectedRecordDefinition().toString() + "\n");

            System.out.println("Creating RDD");
            HpccRDD myRDD = new HpccRDD(sc, parts, rd, hpccFile.getProjectedRecordDefinition());

            System.out.println("Getting local iterator");
            scala.collection.Iterator<Row> rec_iter = myRDD.toLocalIterator();

            int count = 0;
            if (!rec_iter.isEmpty())
            {
                while (rec_iter.hasNext())
                {
                    count++;
                    Row rec = rec_iter.next();
                    System.out.println(rec.toString());
                }
            }

            System.out.println("Completed output of Record data - Total records: " + count);
            System.out.println("End of run");
        }
        catch (Exception e)
        {
            Assert.fail("SimpleRDDTest failed: " + e.getLocalizedMessage());
        }
    }

    @Test
    public void simpleRDDFilteredTest()
    {
        System.out.println("\n----------Spark HPCC RDD Filtered Tests----------");

        try
        {
            try
            {
                hpccFile.setFilter("int8 = 45179");
            }
            catch (Exception e)
            {
                Assert.fail("Failed setting filter: " + e.getLocalizedMessage());
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

            int count = 0;
            if (!rec_iter.isEmpty())
            {
                while (rec_iter.hasNext())
                {
                    count++;
                    Row rec = rec_iter.next();
                    System.out.println(rec.toString());
                }
            }

            System.out.println("Completed output of Record data - Total records: " + count);
            System.out.println("End of run");
        }
        catch (Exception e)
        {
            Assert.fail("SimpleRDDTest failed: " + e.getLocalizedMessage());
        }
    }

    @Test
    public void multipleSetFilterTests()
    {
        System.out.println("\n----------Spark HPCC RDD Filtered Tests----------");
        try
        {
            hpccFile.setFilter("int8 = 45179");
            hpccFile.setFilter("int8 > 45179");
            hpccFile.setFilter("int8 >= 45179");
            hpccFile.setFilter("int8 <= 45179");
            hpccFile.setFilter("int8 IN (45179, 45180)");
            hpccFile.setFilter("int8 IN (45179, 45180) or int8 <= 45179");
            hpccFile.setFilter("int8 IN (45179, 45180) AND int8 <= 45179");
        }
        catch (Exception e)
        {
            Assert.fail("Failed setting filter: " + e.getLocalizedMessage());
        }
    }
}
