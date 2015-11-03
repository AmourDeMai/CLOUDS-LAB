package fr.eurecom.dsg.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
/*
 * Very simple (and scholastic) implementation of a Writable associative map for String to Int
 *
 **/
public class StringToIntMapWritable implements Writable {

    // xTODO: add an internal field that is the real associative array
    public Map<String, Long> instance;
    public Text stringTrans = new Text();
    public LongWritable intTrans = new LongWritable();


    public StringToIntMapWritable(){
        this.instance = new Hashtable<>();
    }

    public void clear(){ this.instance.clear(); }

    public void add(Text input){
        String word = input.toString();
        if(this.instance.containsKey(word)){
            long value = this.instance.get(word) + (long)1;
            this.instance.put(word, value);
        } else {
            this.instance.put(word, (long)1);
        }
    }

    public void add(String word, Long i){
        if(this.instance.containsKey(word)){
            long value = this.instance.get(word) + i;
            this.instance.put(word, value);
        } else {
            this.instance.put(word, i);
        }
    }

    public Set<Map.Entry<String, Long>> entrySet(){
        return this.instance.entrySet();
    }

    public void merge(StringToIntMapWritable input){
        for(Map.Entry<String, Long> entry : input.entrySet()){
            this.add(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        // xTODO: implement deserialization

        // Warning: for efficiency reasons, Hadoop attempts to re-use old instances of
        // StringToIntMapWritable when reading new records. Remember to initialize your variables
        // inside this function, in order to get rid of old data.
        this.clear();
        int numberOfEntries = in.readInt();
        for(int i = 0; i < numberOfEntries; i ++){
            this.stringTrans.readFields(in);
            this.intTrans.readFields(in);
            this.instance.put(this.stringTrans.toString(), this.intTrans.get());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {

        // xTODO: implement serialization
        out.writeInt(this.instance.size());
        for(Map.Entry<String, Long> entry : this.instance.entrySet()){
            this.stringTrans.set(entry.getKey());
            this.intTrans.set(entry.getValue());
            this.stringTrans.write(out);
            this.intTrans.write(out);
        }
    }

    @Override
    public String toString(){
        String value = ":{";
        for (Map.Entry<String, Long> entry : this.entrySet()) {
            value = value + entry.getKey() + ": " + entry.getValue().toString() + "\t";
        }
        value = value + "}";
        return value;
    }
}

