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
package org.hpccsystems.spark.thor;

import java.io.IOException;
import java.nio.charset.Charset;

import org.hpccsystems.spark.HpccFileException;
import org.hpccsystems.spark.RecordDef;

/**
 * The connection to a specific THOR node for a specific file part.
 *
 */
public class PlainConnection {
  private boolean active;
  private boolean closed;
  private boolean simulateFail;
  private boolean forceCursorUse;
  private byte[] cursorBin;
  private int handle;
  private DataPartition dataPart;
  private RecordDef recDef;
  private java.io.DataInputStream dis;
  private java.io.DataOutputStream dos;
  private java.net.Socket sock;
  //
  private static final Charset hpccSet = Charset.forName("ISO-8859-1");
  private static final byte[] hyphen = "-".getBytes(hpccSet);
  private static final byte[] uc_J = "J".getBytes(hpccSet);
  /**
   * A plain socket connect to a THOR node for remote read
   * @param hpccPart the remote file name and IP
   * @param rd the JSON definition for the read input and output
   */
  public PlainConnection(DataPartition dp, RecordDef rd) {
    this.recDef = rd;
    this.dataPart = dp;
    this.active = false;
    this.closed = false;
    this.handle = 0;
    this.cursorBin = new byte[0];
    this.simulateFail = false;
  }
  /**
   * The remote file name.
   * @return file name
   */
  public String getFilename() { return this.dataPart.getFilename(); }
  /**
   * The primary IP for the file part
   * @return IP address
   */
  public String getIP() { return this.dataPart.getPrimaryIP(); }
  /**
   * The port number for the remote read service
   * @return port number
   */
  public int getPort() { return this.dataPart.getClearPort(); }
  /**
   * The read transaction in JSON format
   * @return read transaction
   */
  public String getTrans() { return this.makeInitialRequest(); }
  /**
   * The request string used with a handle
   * @return JSON string
   */
  public String getHandleTrans() { return this.makeHandleRequest(); }
  /**
   * transaction when a cursor is required for the next read.
   * @return a JSON request
   */
  public String getCursorTrans() { return this.makeCursorRequest(); }
  /**
   * Is the read active?
   */
  public boolean isActive() { return this.active; }
  /**
   * Is the remote file closed?  The file is closed after
   * all of the partition content has been transferred.
   * @return true if closed.
   */
  public boolean isClosed() { return this.closed; }
  /**
   * Remote read handle for next read
   * @return the handle
   */
  public int getHandle() { return handle; }
  /**
   * Simulate a handle failure and use the file cursor instead.  The
   * handle is set to an invalid value so the THOR node will indicate
   * that the handle is unknown and request a cursor.
   * @param v true indicates that an invalid handle should be sent
   * to force the fall back to a cursor.  NOTE: this class reads
   * ahead, so the use this before the first read.
   * @return the prior value
   */
  public boolean setSimulateFail(boolean v) {
    boolean old = this.simulateFail;
    this.simulateFail = v;
    return old;
  }
  /**
   * Force the use of cursors instead of handles for testing.
   * @param v the setting
   * @return the previous setting
   */
  public boolean setForceCursorUse(boolean v) {
    boolean old = this.forceCursorUse;
    this.forceCursorUse = v;
    return old;
  }
  /**
   * Read a block of the remote file from a THOR node
   * @return the block sent by the node
   * @throws HpccFileException a problem with the read operation
   */
  public byte[] readBlock()
    throws HpccFileException {
    byte[] rslt = new byte[0];
    if (this.closed) return rslt;    // no data left to send
    if (!this.active) makeActive();  // do the first read
    int len = readReplyLen();
    if (len==0) {
      this.closed = true;
      return rslt;
    }
    if (len < 4 ) {
      throw new HpccFileException("Early data termination, no handle");
    }
    try {
      this.handle = dis.readInt();
      if (this.handle==0) {
        len = retryWithCursor();
        if (len==0) {
          this.closed = true;
          return rslt;
        }
        if (len < 4) {
          throw new HpccFileException("Early data termination on retry, no handle");
        }
        this.handle = dis.readInt();
        if (this.handle==0) {
          throw new HpccFileException("Read retry failed");
        }
      }
    } catch (IOException e) {
      throw new HpccFileException("Error during read block", e);
    }
    try {
      int dataLen = dis.readInt();
      if (dataLen == 0) {
        closeConnection();
        return rslt;
      }
      rslt = new byte[dataLen];
      for (int i=0; i<dataLen; i++) rslt[i] = dis.readByte();
      int cursorLen = dis.readInt();
      if (cursorLen == 0) {
        closeConnection();
        return rslt;
      }
      this.cursorBin = new byte[cursorLen];
      for (int i=0; i<cursorLen; i++) this.cursorBin[i] = dis.readByte();
    } catch (IOException e) {
      throw new HpccFileException("Error during read block", e);
    }
    if (this.simulateFail) this.handle = -1;
    String readAheadTrans = (this.forceCursorUse)
                          ? this.getCursorTrans()
                          : this.getHandleTrans();
    try  {
      int lenTrans = readAheadTrans.length();
      Charset charset = Charset.forName("ISO-8859-1");
      this.dos.writeInt(lenTrans);
      this.dos.write(readAheadTrans.getBytes(charset),0,lenTrans);
      this.dos.flush();
    } catch (IOException e) {
      throw new HpccFileException("Failure sending read ahead transaction", e);
    }
    return rslt;
  }
  /**
   * Open client socket to the primary and open the streams
   * @throws HpccFileException
   */
  private void makeActive() throws HpccFileException{
    this.active = false;
    this.handle = 0;
    this.cursorBin = new byte[0];
    try {
      sock = new java.net.Socket(this.getIP(), this.dataPart.getClearPort());
    } catch (java.net.UnknownHostException e) {
      throw new HpccFileException("Bad file part addr "+this.getIP(), e);
    } catch (java.io.IOException e) {
      throw new HpccFileException(e);
    }
    try {
      this.dos = new java.io.DataOutputStream(sock.getOutputStream());
      this.dis = new java.io.DataInputStream(sock.getInputStream());
    } catch (java.io.IOException e) {
      throw new HpccFileException("Failed to create streams", e);
    }
    this.active = true;
    try {
      Charset charset = Charset.forName("ISO-8859-1");
      String readTrans = makeInitialRequest();
      int transLen = readTrans.length();
      this.dos.writeInt(transLen);
      this.dos.write(readTrans.getBytes(charset),0,transLen);
      this.dos.flush();
    } catch (IOException e) {
      throw new HpccFileException("Failed on initial remote read read trans", e);
    }
  }
  /**
   * Creates a request string using the record definition, filename,
   * and current state of the file transfer.
   * @return JSON request string
   */
  private String makeInitialRequest() {
    StringBuilder sb = new StringBuilder(100
        + this.dataPart.getFilename().length()
        + this.recDef.getJsonInputDef().length()
        + this.recDef.getJsonOutputDef().length());
    sb.append("{ \"format\" : \"binary\", \n");
    sb.append(makeNodeObject());
    sb.append("\n}\n");
    return sb.toString();
  }
  /**
   * Make the node part of the JSON request string
   * @return Json
   */
  private String makeNodeObject() {
    StringBuilder sb = new StringBuilder(50
        + this.dataPart.getFilename().length()
        + this.recDef.getJsonInputDef().length()
        + this.recDef.getJsonOutputDef().length());
    sb.append(" \"node\" : ");
    sb.append("{\n \"kind\" : \"");
    sb.append((this.dataPart.isIndex())? "indexread"  : "diskread");
    sb.append("\",\n \"fileName\" : \"");
    sb.append(this.dataPart.getFilename());
    sb.append("\", \n");
    if (!this.dataPart.getFilter().isEmpty()) {
      sb.append(" ");
      sb.append(this.dataPart.getFilter().toJsonObject());
      sb.append(",\n");
    }
    sb.append(" \"compressed\": \"");
    sb.append((this.dataPart.isCompressed()) ?"true"  :"false");
    sb.append("\", \n \"input\" : ");
    sb.append(this.recDef.getJsonInputDef());
    sb.append(", \n \"output\" : ");
    sb.append(this.recDef.getJsonOutputDef());
    sb.append("\n }");
    return sb.toString();
  }
  /**
   * Request using a handle to read the next block.
   * @return the request as a JSON string
   */
  private String makeHandleRequest() {
    StringBuilder sb = new StringBuilder(100);
    sb.append("{ \"format\" : \"binary\",\n");
    sb.append("  \"handle\" : \"");
    sb.append(Integer.toString(this.handle));
    sb.append("\" \n}");
    return sb.toString();
  }
  private String makeCursorRequest() {
    StringBuilder sb = new StringBuilder(130
        + this.dataPart.getFilename().length()
        + this.recDef.getJsonInputDef().length()
        + this.recDef.getJsonOutputDef().length()
        + (int)(this.cursorBin.length*1.4));
    sb.append("{ \"format\" : \"binary\",\n");
    sb.append(makeNodeObject());
    sb.append(",\n");
    sb.append("  \"cursorBin\" : { \"#valuebin\" : \"");
    sb.append(java.util.Base64.getEncoder().encodeToString(this.cursorBin));
    sb.append("\" }\n}\n ");
    return sb.toString();
  }
  /**
   * Close the connection and clear the references
   * @throws HpccFileException
   */
  private void closeConnection() throws HpccFileException {
    this.closed = true;
    try {
      dos.close();
      dis.close();
      sock.close();
    } catch (IOException e) {}  // ignore this
    this.dos = null;
    this.dis = null;
    this.sock = null;
  }
  /**
   * Read the reply length and process failures if indicated.
   * @return length of the reply less failure indicator
   * @throws HpccFileException
   */
  private int readReplyLen() throws HpccFileException {
    int len = 0;
    boolean hi_flag = false;  // is a response without this set always an error?
    try {
      len = dis.readInt();
      if (len < 0) {
        hi_flag = true;
        len &= 0x7FFFFFFF;
      }
      if (len == 0) return 0;
      byte flag = dis.readByte();
      if (flag==hyphen[0]) {
        if (len<2) throw new HpccFileException("Failed with no message sent");
        int msgLen = len-1;
        byte[] msg = new byte[msgLen];
        this.dis.read(msg);
        String message = new String(msg, hpccSet);
        throw new HpccFileException("Failed with " + message);
      }
      if (flag != uc_J[0]) {
        StringBuilder sb = new StringBuilder();
        sb.append("Invalid response of ");
        sb.append(String.format("%02X ", flag));
        sb.append("received from THOR node ");
        sb.append(this.getIP());
        sb.append(" and return length hi-bit was ");
        sb.append(hi_flag);
        throw new HpccFileException(sb.toString());
      }
      len--;  // account for flag byte read
    } catch (IOException e) {
      throw new HpccFileException("Error during read block", e);
    }
    return len;
  }
  /**
   * Retry with a cursor and read the reply.  Process failures as indicated.
   * @return the length pf the reply less failure indication
   * @throws HpccFileException
   */
  private int retryWithCursor() throws HpccFileException {
    String retryTrans = this.makeCursorRequest();
    int len = retryTrans.length();
    try {
      Charset charset = Charset.forName("ISO-8859-1");
      this.dos.writeInt(len);
      this.dos.write(retryTrans.getBytes(charset),0,len);
      this.dos.flush();
    } catch (IOException e) {
      throw new HpccFileException("Failed on remote read read retry", e);
    }
    return readReplyLen();
  }
}
