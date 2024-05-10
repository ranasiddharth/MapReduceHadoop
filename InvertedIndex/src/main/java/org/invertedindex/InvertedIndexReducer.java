package org.invertedindex;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer
        extends Reducer<Text,Text,Text,Text> {
    /*
    Reduce method collects the output of the Mapper calculate and aggregate the word's count.
    */
    public void reduce(Text key, Iterable<Text> values,
                       Context context
    ) throws IOException, InterruptedException {

        HashMap<String,Integer> map = new HashMap<String,Integer>();
      /*
      Iterable through all the values available with a key [word] and add them together and give the
      final result as the key and sum of its values along with the DocID.
      */
        for (Text val : values) {
            if (map.containsKey(val.toString())) {
                map.put(val.toString(), map.get(val.toString()) + 1);
            } else {
                map.put(val.toString(), 1);
            }
        }
        StringBuilder docValueList = new StringBuilder();
        for(String docID : map.keySet()){
            docValueList.append(docID + ":" + map.get(docID) + " ");
        }
        context.write(key, new Text(docValueList.toString()));
    }
}
