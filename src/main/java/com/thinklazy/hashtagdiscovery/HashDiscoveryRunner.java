package com.thinklazy.hashtagdiscovery;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class HashDiscoveryRunner {
 
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
 
        //Path inputPath = new Path("data/input");
        
    	Path inputPath = new Path("data/instagram_122013_dump.json");
        Path outputDir = new Path("output_data");
 
        // Create configuration
        Configuration conf = new Configuration(true);
 
        // Create job
        Job job = new Job(conf, "HashDiscoveryRunner");
        job.setJarByClass(HashDiscoveryMapper.class);
 
        // Setup MapReduce
        job.setMapperClass(HashDiscoveryMapper.class);
        job.setReducerClass(HashDiscoveryReducer.class);
        job.setNumReduceTasks(1);
 
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(TextInputFormat.class);
 
        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
 
        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
 
    }
 
}