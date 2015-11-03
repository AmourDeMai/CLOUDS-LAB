package fr.eurecom.dsg.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
        Job job = new Job(conf, "Word Co-Occurrence Stripes Pattern");  // xTODO: define new job instead of null using conf e setting a name

        job.setInputFormatClass(TextInputFormat.class);// xTODO: set job input format
        job.setMapperClass(StripesMapper.class);// xTODO: set map class and the map output key and value classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StringToIntMapWritable.class);
        job.setReducerClass(StripesReducer.class);// xTODO: set reduce class and the reduce output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringToIntMapWritable.class);
        job.setCombinerClass(StripesReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);// xTODO: set job output format
        FileInputFormat.addInputPath(job, this.inputPath);// xTODO: add the input file as job input (from HDFS) to the variable inputFile
        FileOutputFormat.setOutputPath(job, this.outputDir);// xTODO: set the output path for the job results (to HDFS) to the variable outputPath
        job.setNumReduceTasks(this.numReducers);// xTODO: set the number of reducers using variable numberReducers
        job.setJarByClass(Stripes.class);// xTODO: set the jar class

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


    public static class StripesMapper
            extends Mapper<LongWritable,   // xTODO: change Object to input key type
            Text,   // xTODO: change Object to input value type
            Text,   // xTODO: change Object to output key type
            StringToIntMapWritable> { // xTODO: change Object to output value type

        private Text first = new Text();
        private Text second = new Text();
        private StringToIntMapWritable count = new StringToIntMapWritable();

        @Override
        public void map(LongWritable offset, // xTODO: change Object to input key type
                        Text line, // xTODO: change Object to input value type
                        Context context)
                throws java.io.IOException, InterruptedException {

            // xTODO: implement map method
            count.clear();
            String[] words = line.toString().split("\\s+");
            for (String word1 : words) {
                count.clear();
                this.first.set(word1);
                for (String word2 : words) {
                    if (!(word2.equals(word1))) {
                        this.second.set(word2);
                        this.count.add(second);
                    }
                }
                context.write(this.first, this.count);
            }
        }
    }

    public static class StripesReducer
            extends Reducer<Text,   // xTODO: change Object to input key type
            StringToIntMapWritable,   // xTODO: change Object to input value type
            Text,   // xTODO: change Object to output key type
            StringToIntMapWritable> { // xTODO: change Object to output value type

        private StringToIntMapWritable sumStringToIntMap = new StringToIntMapWritable();

        @Override
        public void reduce(Text key, // xTODO: change Object to input key type
                           Iterable<StringToIntMapWritable> values, // xTODO: change Object to input value type
                           Context context) throws IOException, InterruptedException {

            sumStringToIntMap.clear();
            // xTODO: implement the reduce method
            for(StringToIntMapWritable i : values){
                sumStringToIntMap.merge(i);
            }
            context.write(key, sumStringToIntMap);
        }
    }
}