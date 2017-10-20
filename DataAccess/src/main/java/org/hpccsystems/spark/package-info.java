/**
 * Spark access to data residing in an HPCC environment.
 * The JAPI class library from HPCCSystems is used to used to access
 * the wsECL services to obtain the location and layout of the file.  An
 * RDD is provided to read the file in parallel by file part.
 *
 * The main classes are:
 * <ul>
 * <li>Content is the abstract class defining field content.  There are concrete
 * classes for each of the different content types. </li>
 * <li>FieldType is an enumeration type listing the types of content.</li>
 * <li>FilePart implements the Spark Partition interface.</li>
 * <li>HpccFile is the metadata for a file on an HPCC THOR cluster.</li>
 * <li>HpccFileException is the general exception class.</li>
 * <li>HpccRDD extends RDD<Record> class for Spark.</li>
 * <li>HpccRemoteFileReader is the facade for the type of file reader.</li>
 * <li>Record is the container class holding the data for a record from THOR.</li>
 * </ul>
 *
 * @author holtjd
 *
 */
package org.hpccsystems.spark;