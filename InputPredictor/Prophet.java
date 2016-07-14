import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by liuyuan on 4/7/16.
 */
public class Prophet {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text reducerKeyAsText = new Text();
        private Text reducerValueAsText = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String inputLine = value.toString();

            Configuration conf = context.getConfiguration();
            int threshold = Integer.parseInt(conf.get("threshold"));

            // Split an input line into phrase and count
            String[] parts = inputLine.split("\t");

            String phrase = parts[0];
            String count = parts[1];

            String[] words = phrase.split(" ");
            int length = words.length;

            // We only need to perform mapreduce to those phrases whose length is greater than 1 and word count greater than threshold.
            if (length > 1 && Integer.parseInt(count) > threshold) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < length - 1; i++) {
                    sb.append(words[i]).append(" ");
                }
                // New key would be the original phrase minus the last word of that phrase.
                String reducerKey = sb.toString().trim();
                // New value would be the last word and phrase count
                String reducerValue = new StringBuilder().append(words[words.length - 1]).append("\t").append(count).toString();
                reducerKeyAsText.set(Bytes.toBytes(reducerKey));
                reducerValueAsText.set(reducerValue);
                context.write(reducerKeyAsText, reducerValueAsText);
            }
        }
    }

    public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int top = Integer.parseInt(conf.get("top"));

            List<PhraseInfo> list = new ArrayList<>();

            int countSum = 0;
            for (Text valueAsText : values) {
                String valueAsString = valueAsText.toString();
                String[] parts = valueAsString.split("\t");
                // Get word and count
                String phrase = parts[0];
                int count = Integer.parseInt(parts[1]);
                // Increase sum count
                countSum += count;
                PhraseInfo phraseInfo = new PhraseInfo(phrase, count);
                // Add the info to list
                list.add(phraseInfo);
            }

            // Sort the list
            Collections.sort(list, new Comparator<PhraseInfo>() {
                @Override
                public int compare(PhraseInfo o1, PhraseInfo o2) {
                    int count1 = o1.getCount();
                    int count2 = o2.getCount();
                    int compareCount = Integer.compare(count2, count1);
                    if (compareCount != 0) {
                        return compareCount;
                    }

                    String phrase1 = o1.getPhrase();
                    String phrase2 = o2.getPhrase();
                    return phrase1.compareTo(phrase2);
                }
            });

            // Get the top 5 or top N if N < 5;
            for (int i = 0; i < list.size() && i < top; i++) {
                double probability = ((double) list.get(i).getCount()) / ((double) countSum);
                Put put = new Put(key.toString().getBytes());
                put.add(Bytes.toBytes("f1"), Bytes.toBytes(list.get(i).getPhrase()), Bytes.toBytes(String.valueOf(probability)));
                context.write(null, put);
            }
        }
    }

    private static class PhraseInfo {

        private String phrase;
        private int count;

        public PhraseInfo (String phrase, int count) {
            this.phrase = phrase;
            this.count = count;
        }

        public int getCount() {
            return count;
        }

        public String getPhrase() {
            return phrase;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        conf.set("threshold", args[1]);
        conf.set("top", args[2]);

        Job job = Job.getInstance(conf, "Prophet");
        job.setJarByClass(Prophet.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(ImmutableBytesWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TableMapReduceUtil.initTableReducerJob("table", Reduce.class, job);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.waitForCompletion(true);
    }
}