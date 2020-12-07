package com.artware.mapreduce.suicide;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, DBInputWritable, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, DBInputWritable value, Context context) throws IOException, InterruptedException {

        try{
            String keys = value.getCountry();
            int valeur = value.getNumber();
            context.write(new Text(keys),new IntWritable(valeur));
        }catch(Exception ignored){
        }
    }
}
