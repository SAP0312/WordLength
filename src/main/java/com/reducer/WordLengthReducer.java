package com.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordLengthReducer extends Reducer<IntWritable, Text,IntWritable,Text> {
    public void reducer(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
        String res = "";
        for(Text value:values){
            if(res.length()==0)
                res=value.toString();
            else
                res = res+","+value.toString();
        }

        context.write(key,new Text(res));


          }
}
