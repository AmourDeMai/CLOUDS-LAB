package fr.eurecom.dsg.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.jdt.core.dom.AST;

import java.io.IOException;

public class OrderInversion extends Configured implements Tool {

    private final static String ASTERISK = "\0";

    public static class PartitionerTextPair extends
            Partitioner<TextPair, IntWritable> {
        @Override
        public int getPartition(TextPair key, IntWritable value,
                                int numPartitions) {
            // TODO: implement getPartition such that pairs with the same first element
            //       will go to the same reducer. You can use toUnsighed as utility.
            Text word = key.getFirst();
            return toUnsigned(word.hashCode()) % numPartitions;
        }

        /**
         * toUnsigned(10) = 10
         * toUnsigned(-1) = 2147483647
         *
         * @param val Value to convert
         * @return the unsigned number with the same bits of val
         */
        public static int toUnsigned(int val) {
            return val & Integer.MAX_VALUE;
        }
    }

    public static class PairMapper extends
            Mapper<LongWritable, Text, TextPair, IntWritable> {

        private IntWritable ONE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws java.io.IOException, InterruptedException {

            // TODO: implement the map method
            String line = value.toString();
            String[] words = line.split("\\s+");

            for (String firstWord : words) {
                for (String secondWord : words) {
                    if (!firstWord.equals(secondWord)) {
                        TextPair pair = new TextPair(firstWord, secondWord);
                        context.write(pair, ONE);
                        pair.set(new Text(firstWord), new Text(ASTERISK));
                        context.write(pair, ONE);
                    }
                }
            }
        }
    }

    public static class PairReducer extends
            Reducer<TextPair, IntWritable, TextPair, DoubleWritable> {

        // TODO: implement the reduce method
        int totalnumber = 0;

        protected void reduce(TextPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            if (key.getSecond().equals(new Text(ASTERISK))) {
                totalnumber = 0;
                for (IntWritable value : values) {
                    totalnumber += value.get();
                }
            } else {
                int count = 0;
                for (IntWritable value : values) {
                    count += value.get();
                }
                context.write(key, new DoubleWritable(count /  totalnumber));
            }
        }
    }

    private int numReducers;
    private Path inputPath;
    private Path outputDir;

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job job = null;  // TODO: define new job instead of null using conf e setting a name
        job = new Job(conf, "Order Inversion");
        // TODO: set job input format
        job.setInputFormatClass(TextInputFormat.class);
        // TODO: set map class and the map output key and value classes
        job.setMapperClass(PairMapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(TextPair.class);
        // TODO: set reduce class and the reduce output key and value classes
        job.setReducerClass(PairReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setPartitionerClass(PartitionerTextPair.class);
        // TODO: set job output format
        job.setOutputFormatClass(TextOutputFormat.class);
        // TODO: add the input file as job input (from HDFS) to the variable inputFile
        FileInputFormat.addInputPath(job, new Path(args[1]));
        // TODO: set the output path for the job results (to HDFS) to the variable outputPath
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // TODO: set the number of reducers using variable numberReducers
        job.setNumReduceTasks(Integer.parseInt(args[0]));
        // TODO: set the jar class
        job.setJarByClass(Pair.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    OrderInversion(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: OrderInversion <num_reducers> <input_file> <output_dir>");
            System.exit(0);
        }
        this.numReducers = Integer.parseInt(args[0]);
        this.inputPath = new Path(args[1]);
        this.outputDir = new Path(args[2]);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrderInversion(args), args);
        System.exit(res);
    }
}
