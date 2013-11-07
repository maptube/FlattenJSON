/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package flattenjson;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayDeque; //it's a stack
import java.util.Iterator;
import java.util.Enumeration;
import java.util.Hashtable;

import com.google.gson.*;
import com.google.gson.stream.*;

/**
 *
 * @author richard
 * @licence
 * Commercial use of this project is restricted. Free for personal use.
 * For licence details see attached licence file. Redistribution is permitted
 * as long as the licence file and credits are preserved.
 * 
 * Depends on the gson parser for JSON.
 * http://code.google.com/p/google-gson/
 * Licence file included with distribution.
 * 
 * @description
 * Take a JSON file as input and flatten it into a CSV file.
 * This is a two pass algorithm where pass one determines the unique column names
 * by flattening the JSON object recursively. Successive depths of value names
 * are prefixed with "::" e.g. a::b::c
 * The second pass fills in the data. Rows are broken where the algorithm tries
 * to fill an element of the row data array, but finds data there already. In other
 * words, it sees another copy of the same column data, so it must be a new row.
 * This is NOT guaranteed to work for all cases. Really, the JSON properties need
 * to be in the same order for successive rows, then it is guaranteed to work.
 * If the order of columns changes between rows then there is no way to break
 * them in the correct place, but this would be unusual. A potential fix is to
 * define a row break column explicitly i.e. if the data we're interested in
 * is always the "r" : { ... } block, then break the row every time you see the
 * "r" name. (version 2?)
 */
public class FlattenJSON {
  //Hold a last in first out stack of json names parsed so far - necessary for maintaining structure of recursive object/array blocks
  protected ArrayDeque<String> nameStack = new ArrayDeque<String>();
  
  //Hold name of all columns found and an index so it's easy to find a column number in the rowdata array for a named column
  protected Hashtable<String,Integer> colindex = new Hashtable<String,Integer>(); //column index lookup
  protected String rowdata[]; //row of data in index order (above) - when we try to fill an already filled slot, it marks a row break
  protected int rowCount; //count the number of rows we output
  
  protected PrintStream csvOut; //output stream for writing to csv file - used in lots of places
  
  private static String usageText =
          "FlattenJSON\n"
          +"Usage: java -jar FlattenJSON [json input file] [csv output file]\n";

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    if (args.length!=2) {
      System.out.println(usageText);
      System.exit(0);
    }
    
    FlattenJSON f = new FlattenJSON();
    //f.flattenJSON("C:\\Users\\richard\\Desktop\\storm-2728Oct2013-dp-cache\\wow_obs\\test_20131028_0759.json");
    //f.flattenJSON("C:\\Users\\richard\\Desktop\\storm-2728Oct2013-dp-cache\\wow_obs\\wow_obs_20131028_0759.json");
    f.flattenJSON(args[0],args[1]);
  }
  
  /**
   * 
   * @param head The head, which is a concatenation (or empty) onto which the tail is added to the end
   * @param tail The tail, which is a simple string json element
   * @return head::tail e.g. a::b::c::d::e  with ::f on the end
   * null + element is just element e.g. "" + f = f, not ::f
   */
  protected String appendTail(String head, String tail) {
    if ((head.length()>0)&&(tail.length()>0)) return head+"::"+tail;
    if ((head.length()>0)) return head; //this copes with anonymous tail blocks
    return tail;
  }
  
  /**
   * Do the actual work
   * @param jsonInFilename
   * @param csvOutFilename
   */
  public void flattenJSON(String jsonInFilename, String csvOutFilename) {
    System.out.println("FlattenJSON: "+jsonInFilename);
    try {
      //csvOut=System.out if you actuall want to see it
      csvOut = new PrintStream(new FileOutputStream(csvOutFilename));
      
      //first pass, get the columns
      colindex = new Hashtable<String,Integer>();
      JsonReader reader = new JsonReader(new FileReader(jsonInFilename));
      reader.setLenient(true);
      flatten(reader,"",true);
      reader.close();
      
      //print out the columns here - they don't come out of the hash in order, 
      //so put them into and array and print that. You could sort, but not much
      //point when you have an index.
      String orderedCols[] = new String[colindex.size()];
      Enumeration<String> keys = colindex.keys();
      while (keys.hasMoreElements()) {
        String colName = keys.nextElement();
        int colIndex = colindex.get(colName).intValue();
        orderedCols[colIndex]=colName;
      }
      //now write them out in the correct order
      for (int i=0; i<orderedCols.length; i++) {
        if (i>0) csvOut.print(",");
        csvOut.print(orderedCols[i]);
      }
      csvOut.println();

      //setup to read data row (with types serialised to strings for csv)
      rowdata = new String[colindex.size()];
      for (int i=0; i<colindex.size(); i++) rowdata[i]=null;

      //second pass, write out data
      rowCount=0;
      reader = new JsonReader(new FileReader(jsonInFilename));
      reader.setLenient(true);
      flatten(reader,"",false);
      writeDataRow(); //you always have a row of data remaining
      reader.close();
      
      csvOut.close();
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
    //System.out.println("Finished.");
    System.out.println("Written "+colindex.size()+" columns and "+rowCount+" rows to "+csvOutFilename);
  }

  /**
   * On second pass through data, handle a data column name and value tuple.
   *
   * @param name
   * @param value
   */
  private void handleData(String name,String value) {
    int idx = colindex.get(name);
    //System.out.println("looking up index "+name+"="+idx);
    if (rowdata[idx]!=null) {
      //already got data for this row, so must be a new one (NOT NECESSARILY!)
      writeDataRow();
    }
    rowdata[idx]=value;
  }

  /**
   * Write out a row of data (CSV) and set the current data row to nulls
   */
  private void writeDataRow() {
    for (int i=0; i<rowdata.length; i++) {
      if (i>0) csvOut.print(",");
      csvOut.print(rowdata[i]);
      rowdata[i]=null; //make sure you nullify the data for the next row
    }
    csvOut.println();
    ++rowCount;
  }
  
  /**
   * Recursive implementation of classic flatten pattern using a json reader to parse tokens.
   * Called as a two-pass algorithm with firstPass=true, then firstPass=false. This populates
   * the columns property so we can differentiate rows.
   * @param reader 
   * @param fqName Fully qualified name i.e. a::b::c
   * @param firstPass On the first pass we're only searching for column headers, on the second pass we're filling in the data
   */
  protected void flatten(JsonReader reader,String fqName,boolean firstPass) {
    String name="",str; //java doesn't keep cases in separate scopes, so define here for all of them
    double num;
    boolean b;
    try {
      while (reader.hasNext()) {
        JsonToken tok = reader.peek();
        switch (tok) {
          case BEGIN_OBJECT:
            //System.out.println("begin object");
            reader.beginObject(); //once you enter beginObject, it only reads to the end of this
            nameStack.push(fqName);
            flatten(reader,appendTail(fqName,name),firstPass);
            fqName = nameStack.pop();
            reader.endObject();
            break;
          case END_OBJECT: //this never gets called!
            //System.out.println("end object");
            break;
          case BEGIN_ARRAY:
            //System.out.println("begin array");
            reader.beginArray(); //once you enter beginArray, it only reads to the end of this
            nameStack.push(fqName);
            flatten(reader,appendTail(fqName,name),firstPass);
            fqName=nameStack.pop();
            reader.endArray();
            break;
          case END_ARRAY: //this never gets called!
            //System.out.println("end array");
            break;
          case NAME:
            name = reader.nextName();
            //System.out.println("name="+name);
            if (firstPass) { //on the first pass we collect up the column names for later
              String colname = appendTail(fqName,name);
              if (!colindex.containsKey(colname)) //make sure you only add the column once
                colindex.put(colname,colindex.size()); //size is the index
            }
            break;
          case STRING:
            str = reader.nextString();
            //System.out.println(appendTail(fqName,name)+"="+str);
            if (!firstPass) handleData(appendTail(fqName,name),str);
            break;
          case NUMBER:
            //there is no way of knowing the type, so you have to decide yourself beforehand
            num = reader.nextDouble();
            //System.out.println(appendTail(fqName,name)+"="+num);
            if (!firstPass) handleData(appendTail(fqName,name),Double.toString(num));
            break;
          case BOOLEAN:
            b = reader.nextBoolean();
            //System.out.println(appendTail(fqName,name)+"="+b);
            if (!firstPass) handleData(appendTail(fqName,name),Boolean.toString(b));
            break;
          case NULL:
            reader.nextNull();
            //System.out.println(appendTail(fqName,name)+"=null");
            if (!firstPass) handleData(appendTail(fqName,name),"null");
            break;
          case END_DOCUMENT:
            return;
        }
      }
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
  }
  
}
