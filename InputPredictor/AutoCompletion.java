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
public class AutoCompletion {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text reducerKeyAsText = new Text();
        private Text reducerValueAsText = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String inputLine = value.toString();
            reducerValueAsText.set(inputLine);

            // Split an input line into phrase and count
            String[] parts = inputLine.split("\t");

            String word = parts[0];
            String count = parts[1];

            String[] words = word.split(" ");
            if (words.length == 1) {
                int length = word.length();
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < length - 1; i++) {
                    sb.append(word.charAt(i));
                    String reducerKey = sb.toString().trim();
                    reducerKeyAsText.set(reducerKey);
                    context.write(reducerKeyAsText, reducerValueAsText);
                }
            }
        }
    }

    public static class Reduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int top = Integer.parseInt(conf.get("top"));
            List<PhraseInfo> list = new ArrayList<>();

            for (Text valueAsText : values) {
                String valueAsString = valueAsText.toString();
                String[] parts = valueAsString.split("\t");
                // Get word and count
                String phrase = parts[0];
                int count = Integer.parseInt(parts[1]);

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
                int count = list.get(i).getCount();
                Put put = new Put(key.toString().getBytes());
                put.add(Bytes.toBytes("f1"), Bytes.toBytes(list.get(i).getPhrase()), Bytes.toBytes(String.valueOf(count)));
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

        conf.set("top", args[1]);

        Job job = Job.getInstance(conf, "AutoCompletion");
        job.setJarByClass(AutoCompletion.class);

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