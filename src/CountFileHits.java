/*
John Karasev
HW#2 Question #1
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

public class CountFileHits {

    private final static IntWritable one = new IntWritable(1);
    /*
        remove " and tokenize the string splitting on white space.
     */
    private static String[] tokenize(Text value) {
        return value.toString()
                .replaceAll("[\"]", "")
                .split("\\s+");
    }
    /*
    Tokenize the Text and returns the index of the file path.
     */
    public static class HitCount extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            word.set(CountFileHits.tokenize(value)[6]); //pass the index of the file
            context.write(word, one);
        }
    }
    /*
        sum up the values of keys.
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
        Job job = Job.getInstance(conf, "hit count");


        job.setJarByClass(CountFileHits.class);

        job.setMapperClass(HitCount.class);

        job.setCombinerClass(IntSumReducer.class); //reduces network traffic.
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

