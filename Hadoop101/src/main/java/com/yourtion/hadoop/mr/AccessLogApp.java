package com.yourtion.hadoop.mr;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Yourtion
 */
public class AccessLogApp {

    private final static LongWritable ONE = new LongWritable(1);


    /**
     * 字符串位置查找
     */
    private static int findStringIndex(String text, String wordToFind, int place) {
        Pattern word = Pattern.compile(wordToFind);
        Matcher match = word.matcher(text);

        int c = 0;
        while (match.find()) {
            c += 1;
            if (c == place) {
                return match.start();
            }
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Path outputPath = new Path(args[1]);

        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = new Job(configuration, "AccessLog");

        job.setJarByClass(AccessLogApp.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setCombinerClass(MyReducer.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value
                    .toString();

            int idx = findStringIndex(log, "\"", 5);
            int end = findStringIndex(log, "\"", 6);
            String usStr = log.substring(idx + 1, end);
            UserAgent ua = UserAgent.parseUserAgentString(usStr);

            context.write(new Text(ua.getBrowser().getName()), ONE);

        }
    }

    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;

            for (LongWritable v : values) {
                sum += v.get();
            }

            context.write(key, new LongWritable((sum)));
        }
    }
}
