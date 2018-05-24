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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import org.hpccsystems.spark.TargetColumn;

/**
 * A type definition object
 */
public class DefEntryType extends DefEntry implements Serializable {
  static final long serialVersionUID = 1L;
  private int used;
  private String name;
  private String childType;
  private ArrayList<DefEntryField> fields;
  private HashMap<String,DefEntryField> fldDict;
  private int start_field_list;
  private int stop_field_list;
  /**
   * Empty constructor for serialization supprt
   */
  protected DefEntryType() {
  }
  /**
   * A DefEntryType for the objects described by the token sequence
   * @param toks the array of tokens from the JSON definition
   * @param startPos the ordinal of the start token
   * @param tokCount the number of tokens
   * @param parent the ordinal of the parent
   */
  public DefEntryType(DefToken[] toks, int startPos, int tokCount, int parent)
      throws UnusableDataDefinitionException {
    super(toks[startPos].getName(), startPos, startPos+tokCount-1, parent);
    this.used = 0;
    this.name = toks[startPos].getName();
    this.childType = "";
    for (int i=startPos+1; i<startPos+tokCount-1; i++) {
      if (DefEntry.CHILD.equals(toks[i].getName())) {
        this.childType = toks[i].getString();
      }
    }
    this.fields = new ArrayList<DefEntryField>();
    this.fldDict = new HashMap<String,DefEntryField>();
    if (DefEntry.hasFields(toks, startPos, tokCount)) {
      int curr_pos = startPos+1;
      while (!DefEntry.FIELDS.equals(toks[curr_pos].getName())) {
        curr_pos++;
      }
      if (!toks[curr_pos].isArrayStart()) {
        StringBuilder sb = new StringBuilder();
        sb.append(DefEntry.FIELDS);
        sb.append(" token was not an array.  Found ");
        sb.append(toks[curr_pos].toString());
        throw new UnusableDataDefinitionException(sb.toString());
      }
      this.start_field_list = curr_pos;
      curr_pos++;
      while(!toks[curr_pos].isArrayEnd()) {
        int num_toks = DefEntry.getTokenCount(toks, curr_pos);
        DefEntryField fld = new DefEntryField(toks, curr_pos, num_toks, startPos);
        this.fields.add(fld);
        this.fldDict.put(fld.getFieldName(), fld);
        curr_pos += num_toks;
      }
      this.stop_field_list = curr_pos;
    } else {
      this.start_field_list = this.getEndPosition() - 1;
      this.stop_field_list = this.getEndPosition();
    }
  }
  /**
   * Count the use of this type and fields defined by this type and named
   * (perhaps implicitly) by the TargetColumn filter
   * @param tc Columns targeted.  If this type has columns and the TargetColumn
   * has no entries, then all columns are selected.
   * @param typDict the type dictionary so referenced types can be updated
   */
  public void countUse(TargetColumn tc, HashMap<String,DefEntryType> typDict)
      throws UnusableDataDefinitionException {
    this.used++;
    if (this.childType!="") {
      if (!typDict.containsKey(this.childType)) {
        throw new UnusableDataDefinitionException("Missing type " + this.childType);
      }
      DefEntryType chld = typDict.get(this.childType);
      chld.countUse(tc, typDict);
    }
    if (this.fields.size()==0) return;
    int marked = 0;
    for (TargetColumn col : tc.getColumns()) {
      if (this.fldDict.containsKey(col.getName())) {
        marked++;
        this.fldDict.get(col.getName()).countUse(col, typDict);
      }
    }
    if (tc.allFields() || marked==0) {
      for (DefEntryField fld : this.fields) {
        fld.countUse(typDict);
      }
    }
  }
  public void countUse(HashMap<String,DefEntryType> typDict)
      throws UnusableDataDefinitionException {
    this.used++;
    if (this.childType!="") {
      if (!typDict.containsKey(this.childType)) {
        throw new UnusableDataDefinitionException("Missing type " + this.childType);
      }
      DefEntryType chld = typDict.get(this.childType);
      chld.countUse(typDict);
    }
    if (this.fields.size()==0) return;
    for (DefEntryField fld : this.fields) {
      fld.countUse(typDict);
    }
  }
  /* (non-Javadoc)
   * @see org.hpccsystems.spark.thor.DefEntry#toTokens(java.util.ArrayList)
   */
  @Override
  public void toTokens(ArrayList<DefToken> toksNew, DefToken[] toksInput) {
    if (this.used==0) return;
    for (int i=this.getBeginPosition(); i<=this.start_field_list; i++) {
      toksNew.add(toksInput[i]);
    }
    for (DefEntryField fld : this.fields) {
      fld.toTokens(toksNew, toksInput);
    }
    for (int i=this.stop_field_list; i<=this.getEndPosition(); i++) {
      toksNew.add(toksInput[i]);
    }
  }
  @Override
  public String toString() {
    int initialSize = 40*(this.getEndPosition()-this.getBeginPosition()+1);
    StringBuilder sb = new StringBuilder(initialSize);
    sb.append("type ");
    sb.append(this.name);
    if (!this.childType.equals("")) {
      sb.append("(");
      sb.append(this.childType);
      sb.append(")");
    }
    sb.append("{");
    sb.append(this.getBeginPosition());
    sb.append("-");
    sb.append(this.getEndPosition());
    sb.append(";");
    sb.append(this.getTokenCount());
    sb.append("} ");
    if (this.fields.size() > 0) {
      sb.append(" [");
      Iterator<DefEntryField> flds = this.fields.iterator();
      while (flds.hasNext()) {
        DefEntryField fld = flds.next();
        sb.append(fld.toString());
        sb.append(";");
      }
      sb.append("]");
    }
    sb.append((this.used>0) ? " used"  : " unused");
    return sb.toString();
  }
}
