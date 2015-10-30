package fr.eurecom.dsg.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

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

public class Stripes extends Configured implements Tool {

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();
        Job job = null;  // TODO: define new job instead of null using conf e setting a name
        job = new Job(conf, "Stripes");
        // TODO: set job input format
        job.setInputFormatClass(TextInputFormat.class);
        // TODO: set map class and the map output key and value classes
        job.setMapperClass(StripesMapper.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(StringToIntMapWritable.class);
        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(StripesReducer.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(StringToIntMapWritable.class);
        // TODO: set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // TODO: add the input file as job input (from HDFS) to the variable inputFile
        FileInputFormat.addInputPath(job, new Path(args[1]));
        // TODO: set the output path for the job results (to HDFS) to the variable outputPath
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // TODO: set the number of reducers using variable numberReducers
        job.setNumReduceTasks(Integer.parseInt(args[0]));
        // TODO: set the jar class
        job.setJarByClass(Stripes.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public Stripes(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: Stripes <num_reducers> <input_path> <output_path>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Stripes(args), args);
        System.exit(res);
    }
}

class StripesMapper
        extends Mapper<LongWritable,   // TODO: change Object to input key type
        Text,   // TODO: change Object to input value type
        Text,   // TODO: change Object to output key type
        StringToIntMapWritable> { // TODO: change Object to output value type

    @Override
    public void map(LongWritable key, // TODO: change Object to input key type
                    Text value, // TODO: change Object to input value type
                    Context context)
            throws IOException, InterruptedException {

        // TODO: implement map method
        String line = value.toString();
        String[] words = line.split("\\s+");

        for (String first : words) {
            StringToIntMapWritable stripe = new StringToIntMapWritable();
            for (String second : words) {
                if (first != second) {
                    if (stripe.containsKey(second)) {
                        int count = stripe.getValueWithStringKey(second);
                        count++;
                        stripe.add(second, count);
                    } else {
                        stripe.add(second, (int)1);
                    }
                }
            }
            context.write(new Text(first), stripe);
        }

    }
}

class StripesReducer
        extends Reducer<Text,   // TODO: change Object to input key type
        StringToIntMapWritable,   // TODO: change Object to input value type
        Text,   // TODO: change Object to output key type
        StringToIntMapWritable> { // TODO: change Object to output value type
    @Override
    public void reduce(Text key, // TODO: change Object to input key type
                       Iterable<StringToIntMapWritable> values, // TODO: change Object to input value type
                       Context context) throws IOException, InterruptedException {

        // TODO: implement the reduce method
        // the value to send in the end
        StringToIntMapWritable stripe = new StringToIntMapWritable();
        HashMap<Text, IntWritable> hashMap = stripe.getHashMap();

        for (StringToIntMapWritable value : values) {
            // get the stripe which is a AssociativeArray, inside in associative array, there's a hashmap
            addValue(hashMap, value.getHashMap());
        }
        stripe.setHashMap(hashMap);
        context.write(key, stripe);
    }

    private void addValue(HashMap<Text, IntWritable> hashMap, HashMap<Text, IntWritable> valueHashMap) {
        Iterator valueIterator = (Iterator) valueHashMap.keySet();
        while (valueIterator.hasNext()) {
            Text word = (Text) valueIterator.next();
            if (hashMap.containsKey(word)) {
              hashMap.put(word, new IntWritable(valueHashMap.get(word).get() + 1));
            } else {
                hashMap.put(word, valueHashMap.get(word));
            }
        }
    }
}