package org.hpccsystems.spark.datasource;

import org.hpccsystems.spark.datasource.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.types.StructType;

import org.hpccsystems.spark.HpccFile;
import org.hpccsystems.spark.SparkSchemaTranslator;

public class HpccRelation extends BaseRelation implements PrunedFilteredScan
{
    private static Logger log        = LogManager.getLogger(HpccRelation.class);

    private HpccFile      file       = null;
    private SQLContext    sqlContext = null;
    private HpccOptions   options    = null;

    public HpccRelation(SQLContext context, HpccOptions opts)
    {
        sqlContext = context;
        options = opts;
    }

    @Override
    public boolean needConversion()
    {
        return true;
    }

    @Override
    public StructType schema()
    {
        if (file == null)
        {
            try
            {
                file = new HpccFile(options.fileName, options.connectionInfo);
                file.setFileAccessExpirySecs(options.expirySeconds);

                if (options.projectList != null)
                {
                    file.setProjectList(options.projectList);
                }
            }
            catch (Exception e)
            {
                String error = "Unable to create HpccRDD with error: " + e.getMessage();
                log.error(error);
                throw new RuntimeException(error);
            }
        }

        StructType schema = null;
        try
        {
            schema = SparkSchemaTranslator.toSparkSchema(file.getProjectedRecordDefinition());
        }
        catch (Exception e)
        {
            String error = "Unable to translate HPCC record defintion to Spark schema:" + e.getMessage();
            log.error(error);
            throw new RuntimeException(error);
        }

        return schema;
    }

    public long sizeInBytes()
    {
        return super.sizeInBytes();
    }

    @Override
    public SQLContext sqlContext()
    {
        return sqlContext;
    }

    @Override
    public Filter[] unhandledFilters(Filter[] filters)
    {
        return filters;
    }

    @Override
    public RDD<Row> buildScan(String[] requiredColumns, Filter[] filters)
    {
        String projectList = String.join(", ", requiredColumns);

        RDD<Row> ret = null;
        try
        {
            if (file == null)
            {
                file = new HpccFile(options.fileName, options.connectionInfo);
                file.setFileAccessExpirySecs(options.expirySeconds);
            }

            file.setFilePartRecordLimit(options.filePartLimit);

            if (options.projectList != null)
            {
                projectList = options.projectList;
            }
            file.setProjectList(projectList);

            if (options.filterString != null)
            {
                file.setFilter(options.filterString);
            }

            ret = file.getRDD(sqlContext.sparkContext());
        }
        catch (Exception e)
        {
            String error = "Unable to create HpccRDD with error: " + e.getMessage();
            log.error(error);
            throw new RuntimeException(error);
        }

        return ret;
    }
}
