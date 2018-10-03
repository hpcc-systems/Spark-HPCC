package org.hpccsystems.spark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.hpccsystems.spark.thor.DataPartition;
import org.hpccsystems.spark.thor.FieldDef;
import org.hpccsystems.spark.thor.FileFilter;
import org.hpccsystems.spark.thor.NullRemapper;
import org.hpccsystems.spark.thor.PlainConnection;
import org.hpccsystems.spark.thor.RemapInfo;

/**
 * Test the access for information on a distributed file on a THOR cluster.
 *
 */
public class HpccFileTest {

  public static void main(String[] args) throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("Enter EclWatch protocol: ");
    System.out.flush();
    String protocol = br.readLine();
    System.out.print("Enter EclWatch ip: ");
    System.out.flush();
    String esp_ip = br.readLine();
    System.out.print("Enter EclWatch port: ");
    System.out.flush();
    String port = br.readLine();
    System.out.print("Enter HPCC file name: ");
    System.out.flush();
    String testName = br.readLine();
    System.out.print("Enter HPCC file cluster name(mythor,etc.): ");
    System.out.flush();
    String fileclustername = br.readLine();
    System.out.print("Enter EclWatch User ID: ");
    System.out.flush();
    String user = br.readLine();
    System.out.print("Enter EclWatch Password: ");
    System.out.flush();
    String pword = br.readLine();
    System.out.print("Enter Project Field list or empty: ");
    System.out.flush();
    String fieldList = br.readLine();
    System.out.print("Enter Record filter expression or empty: ");
    System.out.flush();
    String filterExpression = br.readLine();
    System.out.print("Enter Number of nodes for remap or empty: ");
    System.out.flush();
    String nodes = br.readLine();
    System.out.print("Enter Base IP for remap or empty: ");
    System.out.flush();
    String base_ip = br.readLine();
    System.out.print("Specify file part to read (1 based): ");
    System.out.flush();
    String filePart = br.readLine();

    RemapInfo ri = new RemapInfo(nodes, base_ip);
    HpccFile hpcc = new HpccFile(testName, protocol, esp_ip, port, user, pword, fieldList, new FileFilter(filterExpression), ri, 0, fileclustername);

    System.out.println((hpcc.isIndex())  ? "Index file"  : "Sequential file");
    System.out.println("Getting file parts");
    DataPartition[] parts = hpcc.getFileParts();
    for (int i=0; i<parts.length; i++)
    {
      DataPartition dataPart = parts[i];
      {
        System.out.println(testName + " part " + dataPart.getThisPart() + " of " + dataPart.getNumParts() +":");
        for (int copyindex = 0; copyindex < dataPart.getCopyCount(); copyindex++)
        {
            System.out.print(dataPart.getCopyIP(copyindex) + ":");
        }
        System.out.print(dataPart.getThisPart());
      }
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
    int partIndex = 0;
    try {
      partIndex = Integer.parseInt(filePart) - 1;
      System.out.println("Reading block from part " + filePart);
    } catch(Exception e) {
      System.out.println("Bad input, reading block from part 1");
    }

    DataPartition dataPart = parts[partIndex];
    {
      System.out.println("data partiton " + dataPart.toString());
      PlainConnection pc = new PlainConnection(dataPart, rd);
      System.out.print("Transaction : ");
      System.out.println(pc.getTrans());
      System.out.println(pc.getIP());
      //pc.setSimulateFail(true);
      //pc.setForceCursorUse(true);
      boolean wantData = true;
      int block_limit = 4;
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
          sb.append(String.format("%06d %04X", i, i));
          sb.append("  ");
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
        wantData = block.length > 0 && block_limit-- > 0;
      }
      System.out.println("End data parttion");
    }
    System.out.println("End test");
  }
}
