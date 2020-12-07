package com.artware.mapreduce.suicide;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class Main {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        
        DBConfiguration.configureDB(
                conf,
                "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://localhost:3306/suicide",
                "dbuser",
                "dbpassword"
        );

        Job job = Job.getInstance(conf, "Suicide");

        job.setJarByClass(Main.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(DBOutputWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        String[] tableColumns = new String[]{"country","number"};
        DBInputFormat.setInput(
                job,
                DBInputWritable.class,
                "input",
                null,
                null,
                tableColumns
        );

        DBOutputFormat.setOutput(
                job,
                "output",
                tableColumns);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
