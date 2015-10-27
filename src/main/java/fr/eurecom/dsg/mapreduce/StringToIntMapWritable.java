package fr.eurecom.dsg.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
public class StringToIntMapWritable implements Writable {
  
  // TODO: add an internal field that is the real associative array
  private HashMap<Text, IntWritable> hashMap;

  public StringToIntMapWritable() {
    hashMap = new HashMap<Text, IntWritable>();
  }

  public StringToIntMapWritable(Text text, IntWritable intWritable) {
    this();
    hashMap.put(text, intWritable);
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    
    // TODO: implement deserialization
    Text text = new Text();
    IntWritable intWritable = new IntWritable();

    text.readFields(in);
    intWritable.readFields(in);
    hashMap.put(text, intWritable);
    // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
    // StringToIntMapWritable when reading new records. Remember to initialize your variables 
    // inside this function, in order to get rid of old data.

  }

  @Override
  public void write(DataOutput out) throws IOException {

    // TODO: implement serialization
    Set<Text> keys = hashMap.keySet();
    Iterator iterator = keys.iterator();

    if (iterator.hasNext()) {
      // write key
      Text text = (Text) iterator.next();
      text.write(out);
      // write value
      IntWritable intWritable = hashMap.get(iterator.next());
      intWritable.write(out);
    }
  }


}
