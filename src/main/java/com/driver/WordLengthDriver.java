package com.driver;

import com.mapper.WordLengthMapper;

import com.reducer.WordLengthReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class WordLengthDriver {

    public static void main(String[] args) throws  Exception{
        Configuration configuration = new Configuration();
        Job job = new Job(configuration,"WordLength");
        job.setJarByClass(WordLengthDriver.class);
        job.setMapperClass(WordLengthMapper.class);
        job.setReducerClass(WordLengthReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


//    public static void main(String[] args) throws Exception {
//        Configuration con = new Configuration();
//        String[] files = new GenericOptionsParser(con,args).getRemainingArgs();
//        Path input = new Path(files[0]);
//        Path output = new Path(files[1]);
//
//        Job j =new Job(con,"FindFriend");
//        j.setJarByClass(WordLengthDriver.class);
//        j.setMapperClass(MapForFindFriend.class);
//        j.setReducerClass(ReducerForFindFriend.class);
//        j.setOutputValueClass(Text.class);
//        j.setOutputKeyClass(IntWritable.class);
//        FileInputFormat.addInputPath(j,input);
//        FileOutputFormat.setOutputPath(j,output);
//        System.exit(j.waitForCompletion(true)?0:1);
//
//
//    }
//    public static class MapForFindFriend extends Mapper<LongWritable,Text,IntWritable,Text> {
//        public void map(LongWritable key,Text value, Context con)throws IOException,InterruptedException {
//            String line = value.toString();
//            String[] words = line.split(",");
//            for(String word:words){
//                Text  val= new Text(word);
//                IntWritable  outputKey= new IntWritable(word.length());
//                con.write(outputKey,val);
//            }
//        }
//    }
//
//    public static class ReducerForFindFriend extends Reducer<IntWritable,Text,IntWritable,Text> {
//        public void reduce(IntWritable key,Iterable<Text> value,Context con) throws IOException,InterruptedException{
//            String res="";
//            for(Text t:value){
//                if(res.length()==0)
//
//                    res =t.toString();
//                else
//                    res = res+","+t.toString();
//            }
//            con.write(key,new Text(res));
//        }
//    }
}

