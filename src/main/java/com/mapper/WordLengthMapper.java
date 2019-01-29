package com.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordLengthMapper extends Mapper<LongWritable, Text, IntWritable,Text> {
@Override
public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
        String line = value.toString();
        String[] words = line.split(",");
        for(String str:words) // Splitting text into different words
        {
        // Replacing all non alpha values from words and writing to the context
        context.write(new IntWritable(str.length()),new Text(str.replaceAll("[^A-Za-z]", "")));

        }

        }
}

