package org.hpccsystems.spark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.hpccsystems.spark.thor.BinaryRecordReader;
import org.hpccsystems.spark.thor.DataPartition;
import org.hpccsystems.spark.thor.FieldDef;
import org.hpccsystems.spark.thor.FileFilter;
import org.hpccsystems.spark.thor.RemapInfo;

public class RecordTest {

  public static void main(String[] args) throws Exception{
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
    System.out.print("Field list or empty: ");
    System.out.flush();
    String fieldList = br.readLine();
    System.out.print("File filter expression or empty: ");
    System.out.flush();
    String filterExpression = br.readLine();
    System.out.print("Number of nodes for remap or empty: ");
    System.out.flush();
    String nodes = br.readLine();
    System.out.print("Base IP or empty: ");
    System.out.flush();
    String base_ip = br.readLine();
    HpccFile hpcc;
    if (nodes.equals("") || base_ip.equals("")) {
      hpcc = new HpccFile(testName, protocol, esp_ip, port, user, pword, fieldList,
                          new FileFilter(filterExpression), 0);
    } else {
      RemapInfo ri = new RemapInfo(Integer.parseInt(nodes), base_ip);
      hpcc = new HpccFile(testName, protocol, esp_ip, port, user, pword,
          fieldList, new FileFilter(filterExpression), ri, 0);
    }
    System.out.println("Getting file parts");
    HpccPart[] parts = hpcc.getFileParts();
    for (int i=0; i<parts.length; i++) {
      System.out.println(parts[i].getPartitionInfo().getFilename() + ":"
              + parts[i].getPartitionInfo().getPrimaryIP()+ ":"
              + parts[i].getPartitionInfo().getSecondaryIP() + ": "
              + parts[i].getPartitionInfo().getThisPart());
    }
    System.out.println("Getting record definition");
    RecordDef rd = hpcc.getRecordDefinition();
    FieldDef root_def = rd.getRootDef();
    Iterator<FieldDef> iter = root_def.getDefinitions();
    while (iter.hasNext()) {
      FieldDef field = iter.next();
      System.out.println(field.toString());
    }
    for (int i=0; i<parts.length; i++) {
      System.out.println("Reading records from part index " + i);
      try {
        DataPartition dp = parts[i].getPartitionInfo();
        BinaryRecordReader brr = new BinaryRecordReader(dp, rd);
        while (brr.hasNext()) {
          Record rec = brr.getNext();
          System.out.println(rec.toString());
        }
        System.out.println("completed part at index "+i);
      } catch (Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failed for part ");
        sb.append(parts[i].getPartitionInfo().getThisPart());
        sb.append(" to ");
        sb.append(parts[i].getPartitionInfo().getPrimaryIP());
        sb.append(":");
        sb.append(parts[i].getPartitionInfo().getClearPort());
        sb.append(" with error ");
        sb.append(e.getMessage());
        System.out.println(sb.toString());
      }
    }
    System.out.println("Completed read, end of test");
  }

}
