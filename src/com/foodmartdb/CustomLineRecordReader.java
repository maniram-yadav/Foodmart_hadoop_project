package com.foodmartdb;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class CustomLineRecordReader extends RecordReader{

	private long start;
	private long end;
	private long current_position;
	private int maxLineLength;
	
	private SalesKey key = new SalesKey();
	private SalesValue value = new SalesValue();
	private LineReader in;
	
	
	private static final Log LOG = LogFactory.getLog(CustomLineRecordReader.class);
	
	@Override
	public void close() throws IOException {
		if(in!=null)
			in.close();
		
	}

	@Override
	public SalesKey getCurrentKey() throws IOException, InterruptedException {
		
		return key;
	}

	@Override
	public SalesValue getCurrentValue() throws IOException, InterruptedException {
		
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		if(start==end)
			return 0.0f;
		else
			return Math.min(1.0f, (current_position-start)/(float)(end-start));
	}

	@Override
	public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException, InterruptedException {
		
		FileSplit split= (FileSplit)inputsplit;
		Configuration job = context.getConfiguration();
		
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
							Integer.MAX_VALUE);
		
		start = split.getStart();
		end = start+split.getLength();
		
		final Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());
		
		boolean skipFirstLine = false;
		
		//  check if split point to the start of the file
		if(start==0){
			
			
			skipFirstLine = true;
			--start;
			fileIn.seek(start);
		}
		
		in = new LineReader(fileIn,job);
		if(skipFirstLine){
			
			Text dummy = new Text();
			start += in.readLine(dummy,0, (int) Math.min((long)Integer.MAX_VALUE,
				end-start) );
			
		}
		
		this.current_position = start;
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		Text lineText = new Text();
		int newSize=0;
		while(current_position<end){
			
			newSize = in.readLine(
					lineText,
					maxLineLength,
					Math.max((int)Math.min(1.0f, end-current_position),maxLineLength));

			if(newSize==0)
				break;
			current_position += newSize;
			
			if(newSize < maxLineLength)
			{
				key.set(lineText);
				value.set(lineText);
				break;
			
			}
			
			LOG.info("Skipped line of size "+newSize+" at pos "+ (current_position-newSize));
			
		}
		
		if(newSize==0){
			key = null;
			value = null;
			return false;
		}
		
		return true;
	}

}
