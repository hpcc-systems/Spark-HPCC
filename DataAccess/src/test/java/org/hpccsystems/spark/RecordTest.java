package org.hpccsystems.spark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.spark.sql.Row;
import org.hpccsystems.spark.thor.BinaryRecordReader;
import org.hpccsystems.spark.thor.DataPartition;
import org.hpccsystems.spark.thor.FieldDef;
import org.hpccsystems.spark.thor.FileFilter;
import org.hpccsystems.spark.thor.RemapInfo;

public class RecordTest {

  public static void main(String[] args) throws Exception{
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    System.out.print("Enter EclWatch protocol: ");
    System.out.flush();
    String espprotocol = br.readLine();
    System.out.print("Enter EclWatch ip: ");
    System.out.flush();
    String espip = br.readLine();
    System.out.print("Enter EclWatch port: ");
    System.out.flush();
    String espport = br.readLine();
    System.out.print("Enter HPCC file name: ");
    System.out.flush();
    String testName = br.readLine();
    System.out.print("Enter HPCC file cluster name(mythor,etc.): ");
    System.out.flush();
    String fileclustername = br.readLine();
    System.out.print("Enter EclWatch User ID: ");
    System.out.flush();
    String espuser = br.readLine();
    System.out.print("Enter EclWatch Password: ");
    System.out.flush();
    String esppassword = br.readLine();
    System.out.print("Enter Project Field list or empty: ");
    System.out.flush();
    String projectfildlist = br.readLine();
    System.out.print("Enter Record filter expression or empty: ");
    System.out.flush();
    String filterExpression = br.readLine();
    System.out.print("Enter Number of nodes for remap or empty: ");
    System.out.flush();
    String nodes = br.readLine();
    System.out.print("Enter Base IP for remap or empty: ");
    System.out.flush();
    String base_ip = br.readLine();

    RemapInfo ri = new RemapInfo(nodes, base_ip);
    HpccFile hpccFile = new HpccFile(testName, espprotocol, espip, espport, espuser, esppassword, projectfildlist, new FileFilter(filterExpression), ri, 0, fileclustername);

    System.out.println("Getting file parts");
    DataPartition[] parts = hpccFile.getFileParts();
    for (int i=0; i<parts.length; i++) {
      System.out.println(parts[i].toString());
    }
    System.out.println("Getting record definition");
    RecordDef rd = hpccFile.getRecordDefinition();
    FieldDef root_def = rd.getRootDef();
    Iterator<FieldDef> iter = root_def.getDefinitions();
    while (iter.hasNext()){
      FieldDef field = iter.next();
      System.out.println(field.toString());
    }
    for (int i=0; i<parts.length; i++)
    {
      System.out.println("Reading records from part index " + i);
      try
      {
        BinaryRecordReader brr = new BinaryRecordReader( parts[i], rd);
        while (brr.hasNext()) {
          Row rec = brr.getNext();
          System.out.println(rec.toString());
        }
        System.out.println("Completed file part " +  parts[i].getThisPart());
      }
      catch (Exception e)
      {
        StringBuilder sb = new StringBuilder();
        sb.append("Failed for part ");
        sb.append(parts[i].getThisPart());
        sb.append(" to ");
        sb.append(parts[i].getCopyIP(0));// we might not need this ip...
        sb.append(":");
        sb.append(parts[i].getPort());
        sb.append(" with error ");
        sb.append(e.getMessage());
        System.out.println(sb.toString());
      }
    }
    System.out.println("Completed read, end of test");
  }

}
