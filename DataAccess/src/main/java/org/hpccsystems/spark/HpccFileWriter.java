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
import java.util.ArrayList;
import java.util.Enumeration;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;

import java.util.List;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import java.io.File;
import java.io.FileOutputStream;

import java.nio.channels.FileChannel;
import scala.reflect.ClassTag$;

import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.utils.Connection;

import org.hpccsystems.ws.client.wrappers.wsdfu.DFUCreateFileWrapper;
import org.hpccsystems.ws.client.wrappers.wsdfu.DFUFilePartWrapper;
import org.hpccsystems.ws.client.wrappers.wsdfu.DFUFileCopyWrapper;
import org.hpccsystems.spark.thor.BinaryRecordWriter;
import org.hpccsystems.spark.thor.SparkField;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.execution.python.EvaluatePython;

public class HpccFileWriter implements Serializable {
    static private final long serialVersionUID = 1L;
    static private final int DefaultExpiryTimeSecs = 300;
    static private final Logger log = Logger.getLogger(HpccFileWriter.class.getName());

    // Transient so Java serialization does not try to serialize this
    private transient HPCCWsDFUClient dfuClient = null;

    // Make sure Python picklers have been registered
    static {
        EvaluatePython.registerPicklers();
    }

    /**
    * HpccFileWriter Constructor
    * Attempts to open a connection to the specified HPCC cluster and validates the user.
    * @param connectionString of format {http|https}://{HOST}:{PORT}. Host & port are the same as the ecl watch host & port.
    * @param user a valid ecl watch account
    * @param pass the password for the provided user
    * @throws Exception 
    */
    public HpccFileWriter(String connectionString, String user, String pass) throws Exception {
        // Verify connection & password
        final Pattern connectionRegex = Pattern.compile("(http|https)://([^:]+):([0-9]+)",Pattern.CASE_INSENSITIVE);
        Matcher matches = connectionRegex.matcher(connectionString);
        if (matches.find() == false) {
            throw new Exception("Invalid connection string. Expected format: {http|https}://{HOST}:{PORT}");
        }

        Connection conn = new Connection(matches.group(1), matches.group(2), matches.group(3));
        conn.setUserName(user);
        conn.setPassword(pass);
        this.dfuClient = HPCCWsDFUClient.get(conn);
    }

    private void abortFileCreation() {
        log.error("Abort file creation was called. This is currently a stub.");
    }

    private class PartitionLocation implements Serializable {
        static private final long serialVersionUID = 1L;

        public InetAddress host = null;
        public int partitionIndex = -1;
    }

    private static InetAddress getLocalAddress() throws SocketException {

        // The interfaces arent enumerated in index order and we want
        // the first IP Address on the first valid NetworkInterface
        // So we add the address to a map from Nic Index -> Address
        ArrayList<InetAddress> nicAddressMap = new ArrayList<InetAddress>();
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
        while ( ifaces.hasMoreElements() ) {
            NetworkInterface iface = ifaces.nextElement();
            Enumeration<InetAddress> addresses = iface.getInetAddresses();

            InetAddress validAddress = null;    
            while ( addresses.hasMoreElements() ) {
                InetAddress addr = addresses.nextElement();
                if ( addr instanceof Inet4Address 
                && !addr.isLoopbackAddress() ) {
                    validAddress = addr;
                }
            }

            while (nicAddressMap.size() < iface.getIndex()+1) {
                nicAddressMap.add(null);
            }
            nicAddressMap.set(iface.getIndex(),validAddress);
        }

        // Return first valid address in nic index order
        for (int i = 0; i < nicAddressMap.size(); i++) {
            if (nicAddressMap.get(i) != null) {
                return nicAddressMap.get(i);
            }
        } 

        return null;
    }

    private class FilePartWriteResults implements Serializable {
        static private final long serialVersionUID = 1L;

        public long numRecords = 0;
        public long dataLength = 0;
        public boolean successful = true; // Default to true for empty partitions
    }
    
    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(RDD<Row> scalaRDD, String clusterName, String fileName) throws Exception {
        return this.saveToHPCC(SparkContext.getOrCreate(),scalaRDD,clusterName,fileName);
    }

    /**
    * Saves the provided RDD to the specified file within the specified cluster 
    * @param sc The current SparkContext
    * @param scalaRDD The RDD to save to HPCC
    * @param clusterName The name of the cluster to save to.
    * @param fileName The name of the logical file in HPCC to create. Follows HPCC file name conventions.
    * @return Returns the number of records written
    * @throws Exception 
    */
    public long saveToHPCC(SparkContext sc, RDD<Row> scalaRDD, String clusterName, String fileName) throws Exception {
        
        // Cache the RDD so we get the same partition mapping
        JavaRDD<Row> rdd = JavaRDD.fromRDD(scalaRDD, ClassTag$.MODULE$.apply(Row.class));
        rdd.cache();

        //------------------------------------------------------------------------------
        //  Get current partition to host map
        //------------------------------------------------------------------------------

        Function2<Integer, Iterator<Row>, Iterator<PartitionLocation>> mapFunc = 
        new Function2<Integer, Iterator<Row>, Iterator<PartitionLocation>>() {
            @Override
            public Iterator<PartitionLocation> call(Integer partitionIndex, Iterator<Row> it) throws Exception {
                PartitionLocation location = new PartitionLocation();
                location.host = HpccFileWriter.getLocalAddress();
                location.partitionIndex = partitionIndex;

                List<PartitionLocation> locs = Arrays.asList(location);
                return locs.iterator();
            }
        };

        JavaRDD<PartitionLocation> partitionLocationRdd = rdd.mapPartitionsWithIndex(mapFunc, true);

        List<PartitionLocation> partitionLocations = partitionLocationRdd.collect();
        String[] partitionHostMap = new String[partitionLocations.size()];
        for (PartitionLocation part : partitionLocations) {
            partitionHostMap[part.partitionIndex] = part.host.getHostAddress();
        }

        //------------------------------------------------------------------------------
        //  Request a temp file be created in HPCC to write to
        //------------------------------------------------------------------------------

        // Grab the first row & retrieve its schema for use in create an ECL Record definition
        Row firstRow = scalaRDD.first();
        StructType schema = firstRow.schema();
        StructField tempField = new StructField("root",schema,false,Metadata.empty());
        String eclRecordDefn = SparkField.toECL(new SparkField(tempField));

        DFUCreateFileWrapper createResult = null;
        try {
            createResult = dfuClient.createFile(fileName,clusterName,eclRecordDefn,
                                                partitionHostMap,HpccFileWriter.DefaultExpiryTimeSecs);
        } catch (Exception e) {
            log.error("DFU File Creation Error: " + e.toString());
            throw new Exception("DFU File Creation Error: " + e.toString());
        }

        // Extract filePaths for each partition
        DFUFilePartWrapper[] fileParts = createResult.getFileParts();
        final String[] partitionFilePaths = new String[fileParts.length];
        for (int i = 0; i < fileParts.length; i++) {
            DFUFileCopyWrapper[] filePartCopies = fileParts[i].getCopies();
            if (filePartCopies.length == 0) {
                abortFileCreation();
                throw new Exception("File creation error: File part: " + (i+1) + " does not have an file copies associated with it. Aborting write.");
            }

            // At the moment on the Spark side we are only writing the primary copy.
            // So if more than one copy location is provided we ignore the rest.
            partitionFilePaths[i] = filePartCopies[0].getCopyPath();
        }

        // Validate response
        if (partitionFilePaths.length != rdd.getNumPartitions()) {
            abortFileCreation();
            throw new Exception("File creation error: Invalid number of file parts returned during creation request.");
        }


        //------------------------------------------------------------------------------
        //  Write partitions to file parts
        //------------------------------------------------------------------------------

        Function2<Integer, Iterator<Row>, Iterator<FilePartWriteResults>> writeFunc = 
        (Integer partitionIndex, Iterator<Row> it) -> {
           
            InetAddress localAddress = getLocalAddress();
            boolean addressIsDifferent = localAddress.getHostAddress().equals(partitionHostMap[partitionIndex]) == false;

            // If the address is different we need to bail otherwise write the file part
            FilePartWriteResults result = null;
            if (addressIsDifferent) {
                log.error("File part mapping changed before writing began. Aborting write.");
                result = new FilePartWriteResults();
                result.successful = false;
            } else {
                String filePartPath = partitionFilePaths[partitionIndex];
                result = writeLocalFilePart(filePartPath, it);
            }

            List<FilePartWriteResults> resultList = Arrays.asList(result);
            return resultList.iterator();
        };

        // Create Write Job
        JavaRDD<FilePartWriteResults> writeResultsRDD = rdd.mapPartitionsWithIndex(writeFunc,true);
        List<FilePartWriteResults> writeResultsList = writeResultsRDD.collect();

        long recordsWritten = 0;
        long dataWritten = 0;
        for (int i = 0; i < writeResultsList.size(); i++) {
            FilePartWriteResults result = writeResultsList.get(i);
            recordsWritten += result.numRecords;
            dataWritten += result.dataLength;

            if (result.successful == false) {
                abortFileCreation();
                throw new Exception("Writing failed. An error occured on node: " 
                    + partitionHostMap[i]
                    + " check it's error logs for more information.");
            }
        }
        
        //------------------------------------------------------------------------------
        //  Publish and finalize the temp file
        //------------------------------------------------------------------------------

        try {
            dfuClient.publishFile(createResult.getFileID(),eclRecordDefn,recordsWritten,dataWritten);
        } catch (Exception e) {
            throw new Exception("Failed to publish file with error: " + e.getMessage());
        }

        return recordsWritten;
    }

    private FilePartWriteResults writeLocalFilePart(String filePartPath, Iterator<Row> it) throws Exception
    {
        FilePartWriteResults result = new FilePartWriteResults();

        // Make the parent dir if it does not exist
        String fileWriteDir = filePartPath.substring(0,filePartPath.lastIndexOf(File.separator));
        File dir = new File(fileWriteDir); 
        if (dir.exists() == false) {
            dir.mkdirs();
        }

        FileOutputStream outputStream = new FileOutputStream(filePartPath);
        FileChannel writeChannel = outputStream.getChannel();

        if (it.hasNext()) {

            Row firstRow = it.next();
            try {
                BinaryRecordWriter recordWriter = new BinaryRecordWriter(writeChannel,firstRow.schema());

                recordWriter.writeRecord(firstRow);
                result.numRecords++;

                while (it.hasNext()) {
                    recordWriter.writeRecord(it.next());
                    result.numRecords++;
                }

                recordWriter.finalize();
                result.dataLength = recordWriter.getTotalBytesWritten();
                result.successful = true;
            } catch(Exception e) {
                result.successful = false;
                log.error(e.getMessage());
            }
        }
        
        if (writeChannel != null) {
            writeChannel.close();
        }

        if (outputStream != null) {
            outputStream.close();
        }

        return result;
    }

}
