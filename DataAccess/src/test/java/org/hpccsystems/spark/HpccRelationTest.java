package org.hpccsystems.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.IsNull;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.Or;
import org.apache.spark.sql.sources.StringContains;
import org.apache.spark.sql.sources.StringEndsWith;
import org.apache.spark.sql.sources.StringStartsWith;
import org.hpccsystems.spark.datasource.HpccOptions;
import org.hpccsystems.spark.datasource.HpccRelation;
import org.hpccsystems.ws.client.platform.test.BaseRemoteTest;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public class HpccRelationTest extends BaseRemoteTest
{
    protected final static String sparkMaster = System.getProperty("sparkmaster", "spark://localhost:7077");

    protected static List<Filter> supportedSparkFilters = new ArrayList<Filter>();
    protected static List<Filter> unsupportedSparkFilters = new ArrayList<Filter>();
    protected static HpccOptions hpccopts = null;
    protected static SparkContext sparkcontext = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        supportedSparkFilters.add(new StringStartsWith("fixstr8", "Rod"));
        supportedSparkFilters.add(new Or(new LessThan("int8", 12), new GreaterThan("int8", 8)));
        supportedSparkFilters.add(new In("int8", new Object [] { "str", "values", "etc"}));
        supportedSparkFilters.add(new In("int8", new Object [] { 1, 2, 3, 4, 5.6}));
        supportedSparkFilters.add(new LessThan("fixstr8", "XYZ"));
        supportedSparkFilters.add(new Not(new EqualTo("fixstr8", "true")));
        supportedSparkFilters.add(new EqualTo("int8", 5));
        supportedSparkFilters.add(new Not(new LessThan("int8", 3)));

        unsupportedSparkFilters.add(new IsNull("something"));
        unsupportedSparkFilters.add(new Or(new LessThan("int8", 12), new GreaterThan("int4", 8)));
        supportedSparkFilters.add(new Not(new Or(new LessThan("int8", 12), new GreaterThan("int8", 8))));
        unsupportedSparkFilters.add(new Not(new In("int8", new Object [] { 1, 2, 3, 4, 5.6})));
        unsupportedSparkFilters.add(new StringContains("somestring", "some"));
        unsupportedSparkFilters.add(new StringEndsWith("somestring", "ing"));

        TreeMap<String, String> paramTreeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        paramTreeMap.put("host", connString);
        paramTreeMap.put("path", DEFAULTHPCCFILENAME);
        paramTreeMap.put("cluster", thorClusterFileGroup);//thor_160
        paramTreeMap.put("username", hpccUser);
        paramTreeMap.put("password", hpccPass);

        hpccopts = new HpccOptions(paramTreeMap);
        SparkConf sparkconf = new SparkConf();
        sparkconf.setMaster(sparkMaster).setAppName("SPARKHPCCJUnit");
        sparkcontext = new SparkContext(sparkconf);
    }

//    @Test
//    @Category(org.hpccsystems.commons.annotations.RemoteTests.class)
//    public void testbuildScanAllValid() throws Exception
//    {
//        Assume.assumeTrue("Supported Spark Filters not available", supportedSparkFilters.size() > 0);
//        Assume.assumeTrue("Unsupported Spark Filters not available", unsupportedSparkFilters.size() > 0);
//        Assume.assumeTrue("Spark context not set!", sparkcontext != null);
//        SparkSession sparksession = new SparkSession(sparkcontext);
//        SQLContext sqlcontext = new SQLContext(sparksession);
//
//        Assume.assumeTrue("Spark SQL context not set!", sqlcontext != null);
//        Assume.assumeTrue("hpccopts not set!", hpccopts != null);
//
//        System.out.println("Testing buildScanallvalid");
//        HpccRelation hpccRelation = new HpccRelation(sqlcontext, hpccopts);
//        RDD<Row> rdd = hpccRelation.buildScan(new String[]{"int8"}, supportedSparkFilters.toArray(new Filter[0]));
//    }

    @Test
    public void testUnhandledFiltersAllValid() throws Exception
    {
        Assume.assumeTrue("Supported Spark Filters not available", supportedSparkFilters.size() > 0);

        HpccRelation hpccRelation = new HpccRelation(null, null);
        Filter [] unhandledsparkfilters = hpccRelation.unhandledFilters(supportedSparkFilters.toArray(new Filter[0]));

        Assert.assertTrue("Unexpected unhandled filters detected" , unhandledsparkfilters.length == 0);
    }

    @Test
    public void testUnhandledFiltersNoneValid() throws Exception
    {
        HpccRelation hpccRelation = new HpccRelation(null, null);
        Assume.assumeTrue("Unsupported Spark Filters not available", unsupportedSparkFilters.size() > 0);

        Filter [] unhandledsparkfilters = hpccRelation.unhandledFilters(unsupportedSparkFilters.toArray(new Filter[0]));

        Assert.assertTrue("Unexpected unhandled filters detected" , unhandledsparkfilters.length == unsupportedSparkFilters.size());
    }
}
