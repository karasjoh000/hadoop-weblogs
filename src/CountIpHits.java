/*
John Karasev
HW#2 Question #2
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountIpHits {
    private final static IntWritable one = new IntWritable(1);
    /*
        remove " and tokenize the string splitting on white space(s).
     */
    private static String[] tokenize(Text value) {
        return value.toString()
                .replaceAll("[\"]", "")
                .split("\\s+");
    }
    /*
    tokenizes the Text and returns the index of the ip address.
     */
    public static class IpCount extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            word.set(CountIpHits.tokenize(value)[0]); //index of ip address
            context.write(word, one);
        }
    }
    //count up the values for each key.
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ip hit count");
        job.setJarByClass(CountIpHits.class);
        job.setMapperClass(IpCount.class);
        job.setCombinerClass(IntSumReducer.class); //reduces network traffic
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
