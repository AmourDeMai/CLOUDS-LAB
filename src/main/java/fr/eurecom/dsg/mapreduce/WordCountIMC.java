package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Word Count example of MapReduce job. Given a plain text in input, this job
 * counts how many occurrences of each word there are in that text and writes
 * the result on HDFS.
 * 
 */
public class WordCountIMC extends Configured implements Tool {

  private int numReducers;
  private Path inputPath;
  private Path outputDir;

  @Override
  public int run(String[] args) throws Exception {
      // TODO: define new job instead of null using conf e setting
      Job job = null;
      // TODO: a nameConfiguration
      Configuration conf = this.getConf();
      job = new Job(conf, "Word Count IMC");
      // TODO: set job input format
      job.setInputFormatClass(TextInputFormat.class);
      // TODO: set map class and the map output key and value classes
      job.setMapperClass(WCIMCMapper.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      // TODO: set reduce class and the reduce output key and value classes
      job.setReducerClass(WCIMCReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      // TODO: set job output format
      job.setOutputFormatClass(TextOutputFormat.class);
      // TODO: add the input file as job input (from HDFS)
      FileInputFormat.addInputPath(job, inputPath);
      // TODO: set the output path for the job results (to HDFS)
      FileOutputFormat.setOutputPath(job, outputDir);
      // TODO: set the number of reducers. This is optional and by default is 1
      job.setNumReduceTasks(numReducers);
      // TODO: set the jar class
      job.setJarByClass(WordCountIMC.class);

    return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
  }

  public WordCountIMC (String[] args) {
    if (args.length != 3) {
      System.out.println("Usage: WordCountIMC <num_reducers> <input_path> <output_path>");
      System.exit(0);
    }
    this.numReducers = Integer.parseInt(args[0]);
    this.inputPath = new Path(args[1]);
    this.outputDir = new Path(args[2]);
  }
  
  public static void main(String args[]) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WordCountIMC(args), args);
    System.exit(res);
  }
}

class WCIMCMapper extends Mapper<LongWritable, // TODO: change Object to input key type
                                 Text, // TODO: change Object to input value type
                                 Text, // TODO: change Object to output key type
                                 IntWritable> { // TODO: change Object to output value type

  private HashMap<Text, IntWritable> hashMap;
  private IntWritable ONE = new IntWritable(1);
  private Text textValue = new Text();

  protected void setup(Context context) throws IOException, InterruptedException{
    hashMap = new HashMap<Text, IntWritable>();
  }

  @Override
  protected void map(LongWritable key, // TODO: change Object to input key type
                     Text value, // TODO: change Object to input value type
                     Context context) throws IOException, InterruptedException {

    // * TODO: implement the map method (use context.write to emit results). Use
      String line = value.toString();
      String[] words = line.split("\\s+");
      for (String word : words) {
          textValue.set(word);
          if (hashMap.containsKey(textValue)) {
              int count = hashMap.get(textValue).get();
              count++;
              hashMap.put(textValue, new IntWritable(count));
          } else {
              hashMap.put(textValue, ONE);
          }
      }
    // the in-memory combiner technique

  }

  protected void clearup(Context context) {
      Text text = new Text();
      Set set = hashMap.keySet();
      Iterator iterator = set.iterator();
      while (iterator.hasNext()) {
          text = (Text)iterator.next();
          try {
              context.write(text, hashMap.get(text));
          } catch (IOException e) {
              e.printStackTrace();
          } catch (InterruptedException e) {
              e.printStackTrace();
          }
      }
  }
}

class WCIMCReducer extends Reducer<Text, // TODO: change Object to input key
                                           // type
                                   IntWritable, // TODO: change Object to input value type
                                   Text, // TODO: change Object to output key type
                                   IntWritable> { // TODO: change Object to output value type

  @Override
  protected void reduce(Text key, // TODO: change Object to input key type
                        Iterable<IntWritable> values, // TODO: change Object to input value type
                        Context context) throws IOException, InterruptedException {

    // TODO: implement the reduce method (use context.write to emit results)
      int sum = 0;
      IntWritable writableSum = new IntWritable();

      for (IntWritable value : values) {
          sum += value.get();
      }

      writableSum.set(sum);
      context.write(key, writableSum);
  }
}