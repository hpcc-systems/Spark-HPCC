package org.hpccsystems.spark.thor;

import org.hpccsystems.spark.Content;

/**
 * @author holtjd
 * Container for parsing holding the content object and the items
 * consumed.
 */
public class ParsedContent {
  private final int consumed;
  private final Content item;
  /**
   * construct the holder
   */
  public ParsedContent(Content i, int c) {
    this.consumed = c;
    this.item = i;
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
  public Content getContent() { return item; }
}
