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
import java.util.HashMap;
import org.hpccsystems.spark.TargetColumn;

/**
 * DefEntry for a field in a type with a field list.
 */
public class DefEntryField extends DefEntry implements Serializable {
  static final long serialVersionUID = 1L;
  private String fieldName;
  private String typeName;
  private int used;
  /**
   * Empty constructor to support serialization.
   */
  protected DefEntryField() {
  }
  /**
   * The DefEntry for objects describing a field.
   * @param toks the tokens
   * @param startPos starting position
   * @param tokCount number of tokens in the sequence
   * @param parent the parent position
   */
  public DefEntryField(DefToken[] toks, int startPos, int tokCount, int parent)
      throws UnusableDataDefinitionException {
    super(Integer.toString(startPos), startPos, startPos+tokCount-1, parent);
    this.fieldName = "";
    this.typeName = "";
    for (int i=startPos; i<startPos+tokCount; i++) {
      if (DefEntry.NAME.equals(toks[i].getName())) {
        this.fieldName = toks[i].getString();
      } else if (DefEntry.TYPE.equals(toks[i].getName())) {
        this.typeName = toks[i].getString();
      }
    }
    if ("".equals(this.fieldName) || "".equals(this.typeName)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Unnamed or untyped field at posiiton ");
      sb.append(startPos);
      sb.append(" for ");
      sb.append(tokCount);
      sb.append(" tokens");
      throw new UnusableDataDefinitionException(sb.toString());
    }
  }
  /**
   * The field name associated with the entry.
   * @return the field name.
   */
  public String getFieldName() { return this.fieldName; }
  /**
   * Count the use of this type and fields defined by this type and named
   * (perhaps implicitly) by the TargetColumn filter
   * @param tc Columns targeted.  If this type has columns and the TargetColumn
   * has no entries, then all columns are selected.
   * @param typDict The type dictionary for updating referenced types.
   */
  public void countUse(TargetColumn tc, HashMap<String,DefEntryType> typDict)
      throws UnusableDataDefinitionException {
    this.used++;
    if (!typDict.containsKey(this.typeName)) {
      throw new UnusableDataDefinitionException("Missing type "+this.typeName);
    }
    DefEntryType typ = typDict.get(this.typeName);
    typ.countUse(tc, typDict);
  }
  /**
   * Count the use.  Used when the parent column is marked as all columns.
   * @param typDict the type dictionary so the types can be marked as used
   * @throws UnusableDataDefinitionException
   */
  public void countUse(HashMap<String,DefEntryType> typDict)
      throws UnusableDataDefinitionException {
    this.used++;
    if (!typDict.containsKey(this.typeName)) {
      throw new UnusableDataDefinitionException("Missing type "+this.typeName);
    }
    DefEntryType typ = typDict.get(this.typeName);
    typ.countUse(typDict);
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.thor.DefEntry#toTokens(java.util.ArrayList)
   */
  @Override
  public void toTokens(ArrayList<DefToken> toksNew, DefToken[] toksInput) {
    if (this.used==0) return;
    for (int i=this.getBeginPosition(); i<=this.getEndPosition(); i++) {
      toksNew.add(toksInput[i]);
    }
  }
  @Override
  public String toString() {
    int initialSize = this.fieldName.length()+this.typeName.length()+20;
    StringBuilder sb = new StringBuilder(initialSize);
    sb.append(this.fieldName);
    sb.append("{");
    sb.append(this.getBeginPosition());
    sb.append("-");
    sb.append(this.getEndPosition());
    sb.append(";");
    sb.append(this.getTokenCount());
    sb.append("} of type ");
    sb.append(this.typeName);
    sb.append((this.used>0) ? " used" : " unused");
    return sb.toString();
  }
}
