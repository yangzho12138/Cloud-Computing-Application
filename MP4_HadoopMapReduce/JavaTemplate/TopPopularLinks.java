import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Integer;
import java.util.*;

public class TopPopularLinks extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularLinks(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        //TODO
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("./tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Link Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopPopularLinks.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Links");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(TopLinksMap.class);
        jobB.setReducerClass(TopLinksReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopPopularLinks.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        //TODO

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            IntWritable page = new IntWritable(Integer.parseInt(line.split(":")[0].trim()));
            context.write(page, new IntWritable(0)); // pass this output to reducer

            // linked page
            String[] linkedPages = line.split(":")[1].trim().split(" ");
            for(String linkedPage : linkedPages){
                context.write(new IntWritable(Integer.parseInt(linkedPage)), new IntWritable(1));
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        //TODO

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable value : values){
                count += value.get();
            }
            IntWritable intWritable = new IntWritable(count);
            context.write(key, intWritable); // pass this output to TopTitlesMap mapper
        }
    }

    public static class TopLinksMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
        }

       //TODO
        List<Pair> tops = new ArrayList<>();
        int topNum = 10;

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Pair pair = new Pair(Integer.parseInt(value.toString()), Integer.parseInt(key.toString()));
            tops.add(pair);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Collections.sort(tops);
            Collections.reverse(tops);
            for(int i = 0; i < topNum; i++){
                Integer[] integers = new Integer[2];
                integers[0] = Integer.parseInt(tops.get(i).second.toString());
                integers[1] = Integer.parseInt(tops.get(i).first.toString());
                IntArrayWritable intArrayWritable = new IntArrayWritable(integers);
                context.write(NullWritable.get(), intArrayWritable); // pass this output to reducer
            }
        }
    }

    public static class TopLinksReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
        }
        //TODO

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
            for(IntArrayWritable v : values){
                Writable[] topLinks = v.get();
                IntWritable page = (IntWritable) topLinks[0];
                IntWritable linkNum = (IntWritable) topLinks[1];
                context.write(page, linkNum);
            }
        }
    }
}


class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
