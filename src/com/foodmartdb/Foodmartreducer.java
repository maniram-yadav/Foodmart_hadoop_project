package com.foodmartdb;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class Foodmartreducer extends


Reducer<SalesKey, SalesValue, SalesKey, Text> {

	private MultipleOutputs mos;
	
	public void setup(Context context) {
	 mos = new MultipleOutputs(context);
	 }
	
	public void reduce(SalesKey key, Iterable<SalesValue>  values,Context context)
			throws IOException,InterruptedException
	
	{
		float sun,mon,tue,wed,thu,fri,sat;
		sun=mon=tue=wed=thu=fri=sat=0.0f;
	 
		for(SalesValue v : values){
	    		if(v.getDayOfWeek().toString().equals("Sunday")){
	    			sun += v.getStoreSales().get();
	    		}
	    		else if(v.getDayOfWeek().toString().equals("Monday")){
	    			mon += v.getStoreSales().get();
	    		}
	    		else if(v.getDayOfWeek().toString().equals("Tuesday")){
	    			tue += v.getStoreSales().get();
	    		}
	    		else if(v.getDayOfWeek().toString().equals("Wednesday")){
	    			wed += v.getStoreSales().get();
	    		}
	    		else if(v.getDayOfWeek().toString().equals("Thursday")){
	    			thu += v.getStoreSales().get();
	    		}
	    		else if(v.getDayOfWeek().toString().equals("Friday")){
	    			fri += v.getStoreSales().get();
	    		}
	    		else if(v.getDayOfWeek().toString().equals("Saturday")){
	    			sat += v.getStoreSales().get();
	    		}
	    	}
	    	mos.write("output", key, new Text("Sunday,"+sun));
	    	mos.write("output", key, new Text("Monday,"+mon));
	    	mos.write("output", key, new Text("Tuesday,"+tue));
	    	mos.write("output", key, new Text("Wednesday,"+wed));
	    	mos.write("output", key, new Text("Thursday,"+thu));
	    	mos.write("output", key, new Text("Friday,"+fri));
	    	mos.write("output", key, new Text("Saturday,"+sat));
	
	}

			public void cleanup(Context cntx) throws IOException, InterruptedException {
				mos.close();
			}
}
