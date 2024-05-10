package org.invertedindex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper
        extends Mapper<Object, Text, Text, Text>{

    /*
    Hadoop supported datatypes. This is a hadoop specific datatype that is used to handle
    numbers and Strings in a hadoop environment. IntWritable and Text are used instead of
    Java's Integer and String datatypes.
    Here 'one' is the number of occurance of the 'word' and is set to value 1 during the
    Map process.
    */
    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        // Split DocID and the actual text
        String DocId = value.toString().substring(0, value.toString().indexOf("\t"));
        String value_raw =  value.toString().substring(value.toString().indexOf("\t") + 1);

        // Reading input one line at a time and tokenizing by using space, "'", and "-" characters as tokenizers.
        StringTokenizer itr = new StringTokenizer(value_raw, " '-");

        // Iterating through all the words available in that line and forming the key/value pair.
        while (itr.hasMoreTokens()) {
            // Remove special characters
            word.set(itr.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase());
            if(word.toString() != "" && !word.toString().isEmpty()){
        /*
        Sending to output collector(Context) which in-turn passed the output to Reducer.
        The output is as follows:
          'word1' 5722018411
          'word1' 6722018415
          'word2' 6722018415
        */
                context.write(word, new Text(DocId));
            }
        }
    }
}
