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
package org.hpccsystems.dafilesrv.spark.client;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.List;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import scala.collection.JavaConverters;

public class BinaryRecordWriter
{
    private static final Logger log = Logger.getLogger(BinaryRecordWriter.class.getName());

    private static final int DataLenFieldSize = 4;
    private static final int DefaultBufferSizeKB = 4096;
    private static final byte NegativeSignValue = 0x0d;
    private static final byte PositiveSignValue = 0x0f;

    private ByteChannel outputChannel = null;
    private SeekableByteChannel seekableOutputChannel = null;
    private ByteBuffer buffer = null;
    private long bytesWritten = 0;

    private SparkField extractedSchema = null;
    
    /**
    * BinaryRecordWriter 
    * Initializes writing procedure, and converts the provided Schema to a SparkField stucture
    * @param output ByteChannel to write to. Providing a SeekableOutputChannel provides a performance benefit
    * @param schema The Spark schema used for all subsequent calls to writeRecord
    * @throws Exception
    */
    public BinaryRecordWriter(ByteChannel output, StructType schema) throws Exception
    {
        this.outputChannel = output;
        if(output instanceof SeekableByteChannel) {
            seekableOutputChannel = (SeekableByteChannel) output;
        }

        this.buffer = ByteBuffer.allocateDirect(BinaryRecordWriter.DefaultBufferSizeKB * 1024);
        this.buffer.order(ByteOrder.nativeOrder());

        StructField tempField = new StructField("root",schema,false,Metadata.empty());
        this.extractedSchema = new SparkField(tempField);
    }
    
    /**
    * writeRecord 
    * Converts the provided Row into an HPCC record and writes it to the output channel
    * @param Row
    * @throws Exception
    */
    public void writeRecord(Row row) throws Exception
    {
        for (int i = 0; i < extractedSchema.children.length; i++) {
            Object fieldValue = row.get(i);
            writeField(extractedSchema.children[i],fieldValue);
        }
    }
    
    /**
    * finalize 
    * Must be called after all records have been written.
    * Will flush the internal buffer to the output channel.
    * @throws Exception
    */
    public void finalize() throws Exception
    {
        this.flushBuffer();
    }
    
    /**
    * getTotalBytesWritten 
    * Returns the total bytes written thus far. This will not match the bytes written to the ByteChannel until finialize is called.
    * @return long
    * @throws Exception
    */
    public long getTotalBytesWritten()
    {
        return this.bytesWritten;
    }

    @SuppressWarnings("unchecked")
    private void writeField(SparkField fieldInfo, Object fieldValue) throws Exception
    {
        // If we have less than 32 bytes left in the buffer flush to the channel
        // Note: variable length fields still need to check remaining capacity
        if (this.buffer.remaining() <= 32) {
            this.flushBuffer();
        }

        switch (fieldInfo.type) {
            case ARRAY_TYPE: {
                scala.collection.Seq<Object> seqValue = null;
                if (fieldValue instanceof scala.collection.Seq) {
                    seqValue = (scala.collection.Seq<Object>) fieldValue;
                } else {
                    throw new Exception("Error writing Array. Expected scala.collection.Seq got: " + fieldValue.getClass().getName());
                }

                List<Object> listValue = JavaConverters.seqAsJavaListConverter(seqValue).asJava();
                
                writeArray(fieldInfo, listValue);
                break;
            }
            case BINARY_TYPE: {
                byte[] data = (byte[]) fieldValue;
                long dataSize = data.length;
                writeUnsigned(dataSize);
                writeByteArray(data);
                break;
            }
            case BOOLEAN_TYPE: {
                Boolean value = (Boolean) fieldValue;
                byte byteValue = 0;
                if (value) {
                    byteValue = 1;
                }
                this.buffer.put(byteValue);
                break;
            }
            case BYTE_TYPE: {
                Byte value = (Byte) fieldValue;
                this.buffer.put(value);
                break;
            }
            case DECIMAL_TYPE: {
                BigDecimal value = (BigDecimal) fieldValue;
                writeDecimal(fieldInfo, value);
                break;
            }
            case DOUBLE_TYPE: {
                Double value = (Double) fieldValue;
                this.buffer.putDouble(value);
                break;
            }
            case FLOAT_TYPE: {
                Float value = (Float) fieldValue;
                this.buffer.putFloat(value);
                break;
            }
            case INTEGER_TYPE: {
                Integer value = (Integer) fieldValue;
                this.buffer.putInt(value);
                break;
            }
            case LONG_TYPE: {
                Long value = (Long) fieldValue;
                this.buffer.putLong(value);
                break;
            }
            case SHORT_TYPE: {
                Short value = (Short) fieldValue;
                this.buffer.putShort(value);
                break;
            }
            case STRING_TYPE: {
                String value = (String) fieldValue;
                byte[] data = value.getBytes("UTF-8");
                writeUnsigned(data.length);
                writeByteArray(data);
                break;
            }
            case STRUCT_TYPE: {
                Row row = (Row) fieldValue;
                for (int i = 0; i < fieldInfo.children.length; i++) {
                    writeField(fieldInfo.children[i], row.get(i));
                }
                break;
            }
            default: {
                throw new Exception("Unsupported type encountered while writing field. This should not happen");
            }
        }
    }

    @SuppressWarnings("unchecked")
    private long calculateFieldSize(SparkField fieldInfo,Object fieldValue) throws Exception
    {
        switch (fieldInfo.type) {
            case ARRAY_TYPE: {
                scala.collection.Seq<Object> seqValue = null;
                if (fieldValue instanceof scala.collection.Seq) {
                    seqValue = (scala.collection.Seq<Object>) fieldValue;
                } else {
                    throw new Exception("Error writing Array. Expected scala.collection.Seq got: " + fieldValue.getClass().getName());
                }

                List<Object> listValue = JavaConverters.seqAsJavaListConverter(seqValue).asJava();

                long dataLen = BinaryRecordWriter.DataLenFieldSize;
                boolean isSet = fieldInfo.children[0].type != SparkField.FieldType.STRUCT_TYPE;
                if (isSet) {
                    dataLen++;
                }

                for (Object o : listValue) {
                    dataLen += calculateFieldSize(fieldInfo.children[0], o);
                }

                return dataLen;
            }
            case BINARY_TYPE: {
                byte[] data = (byte[]) fieldValue;
                return data.length + BinaryRecordWriter.DataLenFieldSize;
            }
            case BOOLEAN_TYPE: {
                return 1;
            }
            case BYTE_TYPE: {
                return 1;
            }
            case DECIMAL_TYPE: {
                return (fieldInfo.precision / 2) + 1;
            }
            case DOUBLE_TYPE: {
                return 8;
            }
            case FLOAT_TYPE: {
                return 4;
            }
            case INTEGER_TYPE: {
                return 4;
            }
            case LONG_TYPE: {
                return 8;
            }
            case SHORT_TYPE: {
                return 2;
            }
            case STRING_TYPE: {
                String value = (String) fieldValue;
                byte[] data = value.getBytes("UTF-8");
                return data.length + BinaryRecordWriter.DataLenFieldSize;
            }
            case STRUCT_TYPE: {
                Row row = (Row) fieldValue;

                long dataLen = 0;
                for (int i = 0; i < fieldInfo.children.length; i++) {
                    dataLen += calculateFieldSize(fieldInfo.children[i], row.get(i));
                }
                return dataLen;
            }
            default: {
                throw new Exception("Unsupported type encountered while writing field. This should not happen");
            }
        }
    }

    private void writeArray(SparkField fieldInfo, List<Object> list) throws Exception
    {
        // Data layout for SETS & DATASETS are similar. Exception is SETS have a preceding unused byte.
        boolean isSet = fieldInfo.children[0].type != SparkField.FieldType.STRUCT_TYPE;
        if (isSet) {
            this.buffer.put((byte)0);
        }

        // If we have a seekable channel we can go back and update this value after writing
        // Otherwise we need to calculate the size of the array ahead of writing it
        long dataSetSize = 0;
        if (this.seekableOutputChannel == null) {
            dataSetSize = calculateFieldSize(fieldInfo, list);
            
            // calculated size includes the data size field & and padding byte if present
            // We want only the data size of the child array
            dataSetSize -= BinaryRecordWriter.DataLenFieldSize;
            if (isSet) {
                dataSetSize--;
            }
        }

        // Create a marker to use with the seekable channel if we have one
        long dataSizeMarker = this.buffer.position();
        if (this.seekableOutputChannel != null) {
            dataSizeMarker += this.seekableOutputChannel.position();
        }

        // Write the dataset size to the buffer
        writeUnsigned(dataSetSize);

        for (Object value : list) {
            this.writeField(fieldInfo.children[0],value);
        }

        // If we are using a seekable channel we need to go back and update
        // the dataset size in the channel
        if (this.seekableOutputChannel != null) {
            this.flushBuffer();

            long currentPos = this.seekableOutputChannel.position();
            dataSetSize = currentPos - dataSizeMarker;
            dataSetSize -= BinaryRecordWriter.DataLenFieldSize;

            this.seekableOutputChannel.position(dataSizeMarker);

            // Write the updated dataSize to buffer and then flush for simplicity
            writeUnsigned(dataSetSize);
            this.flushBuffer();
            
            this.seekableOutputChannel.position(currentPos);
        }
    }

    private void writeDecimal(SparkField fieldInfo, BigDecimal decimalValue)
    {
        int mostSigDigit = fieldInfo.precision - fieldInfo.scale;
        boolean isNegative = decimalValue.signum() == -1;
        if (isNegative) {
            decimalValue = decimalValue.negate();
        }

        String decimalStr = decimalValue.stripTrailingZeros().toPlainString();

        // Strip leading zeros. We only want sig digits
        int numLeadingZeros = 0;
        while (decimalStr.charAt(numLeadingZeros) == '0') {
            numLeadingZeros++;
        }
        decimalStr = decimalStr.substring(numLeadingZeros);

        int intDigits = decimalStr.indexOf('.');

        int dataLen = (fieldInfo.precision / 2) + 1;
        byte[] workingBuffer = new byte[dataLen];
        Arrays.fill(workingBuffer,(byte)0);

        // Calculate padding at the beginning. 
        int bitOffset = (mostSigDigit - intDigits) * 4;
        if (bitOffset == 0) {
            bitOffset = 4;
        }

        // Even precision values are shifted by an additional 4 bits
        if (fieldInfo.precision % 2 == 0) {
            bitOffset += 4;
        }

        for (int i = 0; i < decimalStr.length(); i++) {
            if (decimalStr.charAt(i) == '.') {
                continue;
            } 
            int byteOffset = bitOffset / 8;
            int bitShift = (bitOffset+4) % 8;
            
            int digit = (decimalStr.charAt(i) - '0');
            workingBuffer[byteOffset] |= (digit << bitShift);
            bitOffset += 4;
        }

        if (isNegative) {
            workingBuffer[dataLen-1] |= BinaryRecordWriter.NegativeSignValue;
        } else {
            workingBuffer[dataLen-1] |= BinaryRecordWriter.PositiveSignValue;
        }

        this.buffer.put(workingBuffer);
    }

    private void writeUnsigned(long value)
    {
        for (int i = 0; i < 4; i++) {
            int index = i;
            if (this.buffer.order() == ByteOrder.BIG_ENDIAN) {
                index = 3 - i;
            }

            byte byteValue = (byte) ((value >> (index*8)) & 0xff);
            this.buffer.put(byteValue);
        }
    }

    private void writeByteArray(byte[] data) throws Exception
    {
        int dataSize = data.length;
        int offset = 0;
        do {
            int writeSize = dataSize - offset;
            if (writeSize > this.buffer.remaining()) {
                writeSize = this.buffer.remaining();
            }

            this.buffer.put(data,offset,writeSize);
            offset += writeSize;

            if (this.buffer.remaining() <= 32) {
                this.flushBuffer();
            }

        } while(offset < dataSize);
    }

    private void flushBuffer() throws Exception
    {
        // Flip the buffer for reading, write the buffer to the channel and then flip back
        this.buffer.flip();
        do {
            this.bytesWritten += this.outputChannel.write(this.buffer);
        } while( this.buffer.hasRemaining() );
        this.buffer.flip();

        this.buffer.clear();
    }
}
