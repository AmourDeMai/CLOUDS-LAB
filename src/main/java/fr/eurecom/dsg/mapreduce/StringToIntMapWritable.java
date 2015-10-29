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
  private Text word;
  private IntWritable count;

  public StringToIntMapWritable() {
    hashMap = new HashMap<Text, IntWritable>();
    word = new Text();
    count = new IntWritable();
  }

  public StringToIntMapWritable(Text text, IntWritable intWritable) {
    this();
    this.word = text;
    this.count = intWritable;
    hashMap.put(this.word, this.count);
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    
    // TODO: implement deserialization
    IntWritable sizeWritable = new IntWritable();
    sizeWritable.readFields(in);
    int size = sizeWritable.get();

    for (int i = 0; i < size; i++) {
      word = new Text();
      word.readFields(in);  // read the size of word from in
      count = new IntWritable();
      count.readFields(in);
      hashMap.put(word, count);
    }
    // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
    // StringToIntMapWritable when reading new records. Remember to initialize your variables 
    // inside this function, in order to get rid of old data.

  }

  @Override
  public void write(DataOutput out) throws IOException {

    // TODO: implement serialization
    int size = hashMap.size();
    IntWritable sizeWritable = new IntWritable(size);
    sizeWritable.write(out);

    Set<Text> keys = hashMap.keySet();
    Iterator iterator = keys.iterator();
    while (iterator.hasNext()) {
      // write key, write method only write the size of the object bits, here is the size of word
      word = (Text) iterator.next();
      word.write(out);
      // write value
      count = hashMap.get(word);
      count.write(out);
    }
  }

  public HashMap<Text, IntWritable> getHashMap() {
    return hashMap;
  }

  public void setHashMap(HashMap<Text, IntWritable> hashMap) {
    this.hashMap = hashMap;
  }

  public String toString() {
    String result = new String();
    Iterator iterator = hashMap.keySet().iterator();
    while (iterator.hasNext()) {
      Text text = (Text)iterator.next();
      IntWritable intWritable = hashMap.get(iterator.next());
      result += text.toString() + " : " + intWritable.toString();
    }
    return result;
  }
}
