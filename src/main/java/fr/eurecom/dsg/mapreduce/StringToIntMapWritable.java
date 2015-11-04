package fr.eurecom.dsg.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int
 *
 **/
public class StringToIntMapWritable implements Writable {

  // TODO: add an internal field that is the real associative array
  private HashMap<String, Integer> hashMap;
  private Text word;
  private IntWritable count;

  public StringToIntMapWritable() {
    hashMap = new HashMap<String, Integer>();
    word = new Text();
    count = new IntWritable();
  }

  @Override
  public void readFields(DataInput in) throws IOException {

    // TODO: implement deserialization
    hashMap.clear();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      word.readFields(in);
      count.readFields(in);
      hashMap.put(word.toString(), count.get());
    }
    // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
    // StringToIntMapWritable when reading new records. Remember to initialize your variables
    // inside this function, in order to get rid of old data.

  }

  @Override
  public void write(DataOutput out) throws IOException {

    // TODO: implement serialization

    out.writeInt(hashMap.size());
    Iterator iterator = hashMap.keySet().iterator();
    while (iterator.hasNext()) {
      word.set((String) iterator.next());
      count.set(hashMap.get(iterator.next()));
      word.write(out);
      count.write(out);
    }
  }

  public void clear() {
    hashMap.clear();
  }

  public void add(String word) {
    if (hashMap.containsKey(word)) {
      hashMap.put(word, hashMap.get(word) + 1);
    } else {
      hashMap.put(word, 1);
    }
  }

  public void addStripe(StringToIntMapWritable stripe) {
    Iterator iterator = (Iterator) stripe.hashMap.keySet();
    while (iterator.hasNext()) {
      String newWord = (String) iterator.next();
      add(newWord);
    }
  }

  public String toString() {
    String result = " {";
    Iterator iterator = hashMap.keySet().iterator();
    while (iterator.hasNext()) {
      result = iterator.next() + " : " + hashMap.get(iterator.next());
    }
    return result + " }";
  }
}














/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
/*
public class StringToIntMapWritable implements Writable {

  // TODO: add an internal field that is the real associative array
  private HashMap<Text, IntWritable> hashMap;
  private Text word;
  private IntWritable count;

  public StringToIntMapWritable() {
    hashMap = new HashMap<Text, IntWritable>();
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
    // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
    // StringToIntMapWritable when reading new records. Remember to initialize your variables
    // inside this function, in order to get rid of old data.

    hashMap.clear();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      word.readFields(in);  // read the size of word from in
      count.readFields(in);
      hashMap.put(word, count);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {

    // TODO: implement serialization
    out.writeInt(hashMap.size());
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

  public void add(String second, int count) {
    hashMap.put(new Text(second), new IntWritable(count));
  }

  public int getValueWithStringKey(String key) {
    if (containsKey(key)) {
      return hashMap.get(new Text(key)).get();
    } else {
      return 0;
    }
  }

  public void addStripe(StringToIntMapWritable stripe) {
    Iterator iterator = stripe.getHashMap().keySet().iterator();
    while (iterator.hasNext()) {
      Text word = (Text)iterator.next();
      if (this.hashMap.containsKey(word)) {
        this.hashMap.put(word, new IntWritable(this.hashMap.get(word).get() + stripe.hashMap.get(word).get()));
      } else {
        this.hashMap.put(word, stripe.hashMap.get(word));
      }
    }
  }

  public boolean containsKey (String key) {
    return hashMap.containsKey(new Text(key));
  }

  public HashMap<Text, IntWritable> getHashMap() {
    return hashMap;
  }

  public void setHashMap(HashMap<Text, IntWritable> hashMap) {
    this.hashMap = hashMap;
  }

  public String toString() {
    String result = " : {";
    Iterator iterator = hashMap.keySet().iterator();
    while (iterator.hasNext()) {
      word = (Text)iterator.next();
      count = hashMap.get(iterator.next());
      result += word.toString() + " : " + count.toString();
    }
    return result + " }";
  }
}
*/
