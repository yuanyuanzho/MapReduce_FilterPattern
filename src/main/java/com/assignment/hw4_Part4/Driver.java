package com.assignment.hw4_Part4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Hello world!
 *
 */
public class Driver 
{
    public static void main( String[] args ) throws Exception
    {
    	Configuration conf = new Configuration();
        
        if(args.length != 2){
            System.err.println("Usage: InvertedIndex <input> <output>");
            System.exit(2);
        }
        
        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        
        Job job = new Job(conf, "Inverted Index");
        
        job.setJarByClass(FilterMapper.class);
        job.setMapperClass(FilterMapper.class);
       
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);
        
        FileSystem hdfs = FileSystem.get(conf);
        if(hdfs.exists(outputDir)) hdfs.delete(outputDir, true);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
