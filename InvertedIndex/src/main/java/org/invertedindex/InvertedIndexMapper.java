package org.invertedindex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String DocId = value.toString().substring(0, value.toString().indexOf("\t"));
        String value_raw =  value.toString().substring(value.toString().indexOf("\t") + 1);
        StringTokenizer itr = new StringTokenizer(value_raw, " '-");
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
            if(word.toString() != "" && !word.toString().isEmpty()){
                context.write(word, new Text(DocId));
            }
        }
    }
}
