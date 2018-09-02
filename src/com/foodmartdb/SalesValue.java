package com.foodmartdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class SalesValue implements Writable {

	
	private Text day_of_week;
	private FloatWritable StoreSales;
	
	
	public SalesValue(){
		this.day_of_week = new Text();
		this.StoreSales = new FloatWritable();
	}
	
	public SalesValue(Text dofweek,FloatWritable storesales){
		this.day_of_week = dofweek;
		this.StoreSales = storesales;
	}
	
	public void set(Text dofweek,FloatWritable storesales){
		this.day_of_week = dofweek;
		this.StoreSales = storesales;	
	}
	
	public void set(Text txt){
		String values[] = txt.toString().split("|");
		this.day_of_week.set(values[5].trim());
		this.StoreSales.set(Float.parseFloat(values[4].trim()));
	}
	
	public Text getDayOfWeek(){
		return this.day_of_week;
	}
	
	public FloatWritable getStoreSales(){
		return this.StoreSales;
	}
	
	public void setDayOfWeek(Text dow){
		this.day_of_week = dow;
	}
	
	public void setStoreSales(FloatWritable storesales){
		this.StoreSales = storesales;
	} 
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	day_of_week.readFields(in);
	StoreSales.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		day_of_week.write(out);
		StoreSales.write(out);
	}
   
	@Override
	 public String toString(){
		 return this.day_of_week.toString()+","+this.StoreSales.toString();
	 }
	
	

}
