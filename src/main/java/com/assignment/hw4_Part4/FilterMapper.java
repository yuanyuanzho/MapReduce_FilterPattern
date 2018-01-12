package com.assignment.hw4_Part4;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FilterMapper extends Mapper<Object, Text, NullWritable, Text>{

	private String regex = ".+404.+";

	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		if(value.toString().matches(regex)){
			context.write(NullWritable.get(), value);
		}
	}
}
