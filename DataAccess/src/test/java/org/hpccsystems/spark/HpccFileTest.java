package org.hpccsystems.spark;

import java.util.Iterator;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.hpccsystems.spark.thor.FieldDef;
import org.hpccsystems.spark.thor.RemapInfo;

/**
 * Test the access for information on a distributed file on a THOR cluster.
 * @author John Holt
 *
 */
public class HpccFileTest {

  public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("Enter protocol: ");
    System.out.flush();
    String protocol = br.readLine();
    System.out.print("Enter ip: ");
    System.out.flush();
    String esp_ip = br.readLine();
    System.out.print("Enter port: ");
    System.out.flush();
    String port = br.readLine();
    System.out.print("Enter file name: ");
    System.out.flush();
    String testName = br.readLine();
    System.out.print("User id: ");
    System.out.flush();
    String user = br.readLine();
    System.out.print("pass word: ");
    System.out.flush();
    String pword = br.readLine();
    System.out.print("Number of nodes for remap or empty: ");
    System.out.flush();
    String nodes = br.readLine();
    System.out.print("Base IP or empty: ");
    System.out.flush();
    String base_ip = br.readLine();
    System.out.print("Project list (comma delimited): ");
    System.out.flush();
    String projectList = br.readLine();

    HpccFile hpcc;
    if (nodes.equals("") || base_ip.equals("")) {
      hpcc = new HpccFile(testName, protocol, esp_ip, port, user, pword, projectList);
    } else {
      RemapInfo ri = new RemapInfo(Integer.parseInt(nodes), base_ip);
      hpcc = new HpccFile(testName, protocol, esp_ip, port, user, pword, projectList, ri);
    }
    System.out.println("Getting file parts");
    FilePart[] parts = hpcc.getFileParts();
    for (int i=0; i<parts.length; i++) {
      System.out.println(parts[i].getFilename() + ":"
              + parts[i].getPrimaryIP()+ ":"
              + parts[i].getSecondaryIP() + ": "
              + parts[i].getThisPart());
    }
    System.out.println("Getting JSON definition");
    System.out.println(hpcc.getRecordDefinition().getJsonInputDef());
    System.out.println("Getting record definition");
    RecordDef rd = hpcc.getRecordDefinition();
    FieldDef root_def = rd.getRootDef();
    Iterator<FieldDef> iter = root_def.getDefinitions();
    while (iter.hasNext()) {
      FieldDef field = iter.next();
      System.out.println(field.toString());
    }
    System.out.print("Schema: ");
    System.out.println(rd.asSchema().toString());
    System.out.println("Reading block from part 2");
    org.hpccsystems.spark.thor.PlainConnection pc
          = new org.hpccsystems.spark.thor.PlainConnection(parts[1], rd);
    System.out.print("Transaction : ");
    System.out.println(pc.getTrans());
    System.out.println(pc.getIP());
    System.out.println(pc.getFilename());
    //pc.setSimulateFail(true);
    boolean wantData = true;
    while (wantData) {
      byte[] block = pc.readBlock();
      StringBuilder sb = new StringBuilder();
      sb.append("Handle ");
      sb.append(pc.getHandle());
      sb.append(", data length=");
      sb.append(block.length);
      System.out.println(sb.toString());
      for (int i=0; i<block.length; i+=16) {
        sb.delete(0, sb.length());
        for (int j=0; j<16 && i+j<block.length; j++) {
          sb.append(String.format("%02X ", block[i+j]));
          sb.append(" ");
        }
        System.out.println(sb.toString());
      }
      if (pc.isClosed()) System.out.println("Closed connection");
      else {
        System.out.print("Handle trans is ");
        System.out.println(pc.getHandleTrans());
        System.out.println("CursorBin transaction is: ");
        System.out.println(pc.getCursorTrans());
      }
      wantData = block.length > 0;
    }
    System.out.println("End test");
  }
}
