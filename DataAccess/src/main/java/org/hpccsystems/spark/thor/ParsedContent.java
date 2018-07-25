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

/**
 * Container for parsing holding the content object and the items
 * consumed.
 */
public class ParsedContent {
  private final int consumed;
  private final Object[] fields;
  /**
   * construct the holder
   */
  public ParsedContent(Object[] f, int c) {
    this.fields = f;
    this.consumed = c;
  }
  /**
   * The number of input units consumed to make this content item.
   * @return bytes if from a byte stream, tokens if a token stream
   */
  public int getConsumed() { return consumed; }
  /**
   * The content item
   * @return item
   */
  public Object[] getFields() { return fields; }
}
