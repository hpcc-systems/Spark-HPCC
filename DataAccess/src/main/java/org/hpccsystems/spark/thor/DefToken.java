package org.hpccsystems.spark.thor;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonLocation;
import java.util.Deque;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Enumeration;

public class DefToken {
  private final JsonToken tok;
  private final String whenString;
  private final double whenReal;
  private final long whenInteger;
  private final boolean whenBoolean;
  private final Integer parent;
  private final String name;
  // private constructors
  private DefToken(JsonToken tok, Integer parent, String name) {
    this.tok = tok;
    this.parent = parent;
    this.name = name;
    this.whenString = "";
    this.whenReal = 0;
    this.whenInteger = 0;
    this.whenBoolean = false;
  }
  private DefToken(JsonToken tok, Integer parent, String name, String s) {
    this.tok = tok;
    this.parent = parent;
    this.name = name;
    this.whenString = s;
    this.whenReal = 0;
    this.whenInteger = 0;
    this.whenBoolean = false;
  }
  private DefToken(JsonToken tok, Integer parent, String name, double d) {
    this.tok = tok;
    this.parent = parent;
    this.name = name;
    this.whenString = "";
    this.whenReal = d;
    this.whenInteger = 0;
    this.whenBoolean = false;
  }
  private DefToken(JsonToken tok, Integer parent, String name, long l) {
    this.tok = tok;
    this.parent = parent;
    this.name = name;
    this.whenString = "";
    this.whenReal = 0;
    this.whenInteger = l;
    this.whenBoolean = false;
  }
  private DefToken(JsonToken tok, Integer parent, String name, boolean b) {
    this.tok = tok;
    this.parent = parent;
    this.name = name;
    this.whenString = "";
    this.whenReal = 0;
    this.whenInteger = 0;
    this.whenBoolean = b;
  }
  // access methods
  /**
   * @return the JsonToken enumeration value
   */
  public JsonToken getToken() { return tok; }
  /**
   * @return the position in the array of the parent
   */
  public Integer getParent() { return parent; }
  /**
   * @return the name of this token
   */
  public String getName() { return name; }
  /**
   * @return the string value if token was a string
   */
  public String getString() { return whenString; }
  /**
   * @return the floating point value if the type was REAL
   */
  public double getReal() { return whenReal; }
  /**
   * @return the integer value if the value was an integer
   */
  public long getInteger() { return whenInteger; }
  /**
   * @return the boolean value if the token was a true or false
   */
  public boolean getBoolean() { return whenBoolean; }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(parent);
    sb.append(" ");
    sb.append(name);
    sb.append("=");
    sb.append(tok.toString());
    switch(tok) {
      case VALUE_NUMBER_INT:
        sb.append(" ");
        sb.append(Long.toString(whenInteger));
        break;
      case VALUE_NUMBER_FLOAT:
        sb.append(" ");
        sb.append(Double.toString(whenReal));
        break;
      case VALUE_STRING:
        sb.append(":");
        sb.append(whenString);
        break;
      case VALUE_TRUE:
      case VALUE_FALSE:
        sb.append(" ");
        sb.append(Boolean.toString(whenBoolean));
        break;
      default:
        break;
    }
    return sb.toString();
  }
  /**
   * Parse the JSON encoded definition string into an array of
   * tokens.  The def string should never cause an IOException,
   * so if an IOException occurrs and Illegal Argument Exception
   * is thrown because nothing can be done.
   * @param def a string from an HPCC cluster with the record
   * layout information
   * @return an array of tokens
   * @throws JsonParseException
   */
  public static ArrayList<DefToken> parseDefString(String def)
            throws JsonParseException {
    ArrayList<DefToken> tokens = new ArrayList<DefToken>();
    Integer parent = new Integer(-1);
    Deque<Integer> parent_stack = new ArrayDeque<Integer>();
    JsonFactory factory = new JsonFactory();
    JsonParser jp = null;
    JsonToken tok = null;
    try {
      jp = factory.createParser(def);
      tok = jp.nextValue();
      if (tok!=JsonToken.START_OBJECT) {
        StringBuilder sb = new StringBuilder();
        sb.append("Expected Start Object, found ");
        sb.append(tok.toString());
        JsonLocation loc = jp.getTokenLocation();
        throw new JsonParseException(sb.toString(), loc);
      }
      DefToken curr = null;
      while (tok!=null) {
        if (tok==JsonToken.END_OBJECT || tok==JsonToken.END_ARRAY) {
          parent = parent_stack.pop();
        }
        String name = jp.getCurrentName();
        switch (tok) {
          case VALUE_NUMBER_INT:
            curr = new DefToken(tok, parent, name, jp.getLongValue());
            break;
          case VALUE_NUMBER_FLOAT:
            curr = new DefToken(tok, parent, name, jp.getDoubleValue());
            break;
          case VALUE_TRUE:
          case VALUE_FALSE:
            curr = new DefToken(tok, parent, name, jp.getBooleanValue());
            break;
          case VALUE_STRING:
            curr = new DefToken(tok, parent, name, jp.getText());
            break;
          default:
            curr = new DefToken(tok, parent, name);
        }
        tokens.add(curr);
        if (tok==JsonToken.START_OBJECT || tok==JsonToken.START_ARRAY) {
          parent_stack.push(parent);
          parent = new Integer(tokens.size()-1);
        }
        tok = jp.nextValue();
      }
    } catch (IOException e) {
      String msg = "JSON Parse triggered IO exception";
      throw new IllegalArgumentException(msg, e);
    }
    return tokens;
  }
}
