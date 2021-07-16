package org.hpccsystems.spark.datasource;

import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.PrunedFilteredScan;
import org.apache.spark.sql.types.StructType;
import org.hpccsystems.commons.ecl.FileFilter;
import org.hpccsystems.spark.FileFilterConverter;
import org.hpccsystems.spark.HpccFile;
import org.hpccsystems.spark.SparkSchemaTranslator;

/**
 * Represents a dataset in HPCC Systems with a known schema.
 */
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
        List<Filter> unhandledFilters = new ArrayList<Filter>();
        for (Filter filter : filters)
        {
            try
            {
                FileFilter filefilter = FileFilterConverter.ConvertToHPCCFileFilterString(filter); //somewhat expensive action
                if (filefilter != null && !filefilter.isEmpty())
                    continue;
            }
            catch (Exception e) {}
            unhandledFilters.add(filter);
        }

        return unhandledFilters.toArray(new Filter[0]);
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

            if (filters != null && filters.length != 0)
            {
                try
                {
                    FileFilter filefilter = FileFilterConverter.CovertToHPCCFileFilter(filters);
                    if (filefilter != null && !filefilter.isEmpty())
                        file.setFilter(filefilter);
                }
                catch (Exception e)
                {
                    log.error("Could not apply filter(s) to File '" + file.getFileName() + "': " + e.getLocalizedMessage() );
                }
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

    public boolean equals(Object rhs)
    {
        if (rhs instanceof HpccRelation)
        {
            HpccRelation otherRelation = (HpccRelation) rhs;
            HpccOptions otherOptions = otherRelation.options;
            
            boolean nameMatches = otherOptions.fileName.equals(options.fileName);

            boolean projectListMatches = true;
            if (otherOptions.projectList != null)
            {
                if (options.projectList != null)
                {
                    projectListMatches = otherOptions.projectList.equals(options.projectList);
                }
                else
                {
                    return false;
                }
            }

            // The filter string can be overriden during buildScan, but this will only lead to
            // false negatives. So we don't have to worry about that.
            boolean filterStringMatches = true;
            if (otherOptions.filterString != null)
            {
                if (options.filterString != null)
                {
                    otherOptions.filterString.equals(options.filterString);
                }
                else
                {
                    return false;
                }
            }

            boolean hostMatches = options.connectionInfo.getHost().equals(otherOptions.connectionInfo.getHost());
            boolean filePartLimitMatches = otherOptions.filePartLimit == options.filePartLimit;

            return hostMatches && nameMatches && projectListMatches && filterStringMatches && filePartLimitMatches;
        }

        return false;
    }
}
