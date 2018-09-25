import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxHitFile {

    private final static IntWritable one = new IntWritable(1);

    private static String[] tokenize(Text value) {
        return value.toString()
                .replaceAll("[\\[\\]\"]", "")
                .split("\\s+");
    }

    public static class HitCount extends Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            word.set(MaxHitFile.tokenize(value)[6]);
            context.write(word, one);
        }
    }


    public static class MaxHit extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        private static int max = 0;
        private static Text maxkey;
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            if(sum > max) {
                max = sum;
                maxkey = key;
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException {
            result.set(max);
            context.write(maxkey, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max hit file");

        job.setJarByClass(MaxHitFile.class);

        job.setMapperClass(HitCount.class);

        job.setCombinerClass(MaxHit.class);
        job.setReducerClass(MaxHit.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
