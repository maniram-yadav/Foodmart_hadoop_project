package com.foodmartdb;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FoodmartInputFormat extends FileInputFormat{

	
	@Override
	public RecordReader createRecordReader(InputSplit split, 
			TaskAttemptContext arg1)
			throws IOException, InterruptedException {

		return new CustomLineRecordReader();
	}

}
