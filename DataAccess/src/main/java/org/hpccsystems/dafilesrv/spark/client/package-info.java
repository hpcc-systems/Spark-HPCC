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
 * <li>HpccPart implements the Spark Partition interface.</li>
 * <li>HpccFile is the metadata for a file on an HPCC THOR cluster.</li>
 * <li>HpccFileException is the general exception class.</li>
 * <li>HpccRDD extends RDD<Record> class for Spark.</li>
 * <li>HpccRemoteFileReader is the facade for the type of file reader.</li>
 * <li>Record is the container class holding the data for a record from THOR.</li>
 * </ul>
 *
 */
package org.hpccsystems.dafilesrv.spark.client;