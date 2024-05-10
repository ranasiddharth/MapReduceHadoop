package org.distributedgrep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GrepMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final static Text location = new Text();
    private String grepWord;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        grepWord = context.getConfiguration().get("grep.word");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");
        for (String word : words) {
            if (word.equals(grepWord)) {
                location.set("Found in document: " + context.getInputSplit() + ", content: " + line);
                context.write(new Text(word), location);
            }
        }
    }
}
