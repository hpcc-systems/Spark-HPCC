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
import java.util.ArrayList;
import java.util.Iterator;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.hpccsystems.spark.thor.DefEntryRoot;
import org.hpccsystems.spark.thor.DefToken;
import org.hpccsystems.spark.thor.DefEntry;
import org.hpccsystems.spark.thor.DefEntryRoot;
import org.hpccsystems.spark.thor.FieldDef;
import org.hpccsystems.spark.thor.RemapInfo;
import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.ws.client.utils.Connection;

public class PruneTest {

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
    String fileName = br.readLine();
    System.out.print("User id: ");
    System.out.flush();
    String user = br.readLine();
    System.out.print("pass word: ");
    System.out.flush();
    String pword = br.readLine();
    System.out.print("Field list or empty: ");
    System.out.flush();
    String fieldList = br.readLine();
    // pick up DFU info
    Connection conn = new Connection(protocol, esp_ip, port);
    conn.setUserName(user);
    conn.setPassword(pword);
    HPCCWsDFUClient hpcc = HPCCWsDFUClient.get(conn);
    System.out.println("Getting Json record definition");
    DFUFileDetailInfo fd = hpcc.getFileDetails(fileName, "", true, false);
    String defThor = fd.getJsonInfo();
    System.out.println(defThor);
    DefToken[] toks = DefToken.parseDefString(defThor);
    System.out.println("JSON string from tokens:");
    StringBuffer orig_sb = new StringBuffer();
    for (DefToken tok : toks) {
      orig_sb.append(tok.toJson());
    }
    System.out.println(orig_sb.toString());
    System.out.println("Tokens");
    for (int i=0; i<toks.length; i++) {
      System.out.println(toks[i].toString());
    }
    System.out.println("Pruned tokens:");
    ColumnPruner cp = new ColumnPruner(fieldList);
    TargetColumn tcRoot = cp.getTargetColumn();
    DefEntryRoot root = new DefEntryRoot(toks);
    root.countUse(tcRoot);
    System.out.println(root.toString());
    ArrayList<DefToken> work_list = new ArrayList<DefToken>();
    root.toTokens(work_list, toks);
    DefToken[] pruned_toks = work_list.toArray(new DefToken[0]);
    DefToken.renumber(pruned_toks);
    for (DefToken tok : pruned_toks) System.out.println(tok.toString());
    System.out.println("Output JSON string");
    StringBuilder def_sb = new StringBuilder();
    for (DefToken tok : pruned_toks) {
      def_sb.append(tok.toJson());
    }
    System.out.println(def_sb.toString());
    System.out.println("Done");
  }
  // end test
}
