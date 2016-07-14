import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by liuyuan on 4/7/16.
 */
public class NGram {
    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String inputLine = value.toString();

            // To lower case.
            inputLine = inputLine.toLowerCase();

            // Replace ref tags.
            inputLine = inputLine.replaceAll("<ref[^>]*>", " ");
            inputLine = inputLine.replaceAll("</ref[^>]*>", " ");

            // Replace urls.
            inputLine = inputLine.replaceAll("(https?|ftp)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]", " ");


            // Replace non-letters.
            inputLine = inputLine.replaceAll("[^a-z'\n]", " ");

            // Deal with "'"
            inputLine = inputLine.replaceAll("'(?![a-z])", " ");
            inputLine = inputLine.replaceAll("(?<![a-z])'", " ");

            // Deal with spaces
            // inputLine = inputLine.replaceAll("[^\\S\\r\\n]+", " ");
            inputLine = inputLine.replaceAll("[ ]+", " ");
            inputLine = inputLine.trim();

            String[] parts = inputLine.split(" +");
            int length = parts.length;

            for (int i = 0; i < length; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = i; j > i - 5 && j >= 0; j--) {
                    sb.insert(0, parts[j] + " ");
                    String gram = sb.toString().trim();
                    if (gram.length() != 0) {
                        word.set(gram);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                    sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "NGram");
        job.setJarByClass(NGram.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}