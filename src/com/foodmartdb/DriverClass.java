package com.foodmartdb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DriverClass {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf= new Configuration();
		
		Job job = new Job(conf,"Foodmart DB Project");
		
		System.out.println("Setting jar");
		job.setJarByClass(DriverClass.class);
		job.setMapperClass(FoodmartMapper.class);
		job.setReducerClass(Foodmartreducer.class);
		
		System.out.println("Setting output key");
		job.setOutputKeyClass(SalesKey.class);
		job.setOutputValueClass(String.class);
		
		System.out.println("Setting format");
		job.setInputFormatClass(FoodmartInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(job,"output", TextOutputFormat.class,
				SalesKey.class,Text.class);
		
		System.out.println("Setting path");
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
