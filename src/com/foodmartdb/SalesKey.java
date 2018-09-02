package com.foodmartdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class SalesKey implements WritableComparable {

	private IntWritable region_id;
	private IntWritable PromotionID;
	private IntWritable sales_year;
	private IntWritable sales_month;
	
	public SalesKey(){
		this.region_id =  new IntWritable(-1);
		this.PromotionID = new IntWritable(-1);
		this.sales_month = new IntWritable(-1);
		this.sales_year = new IntWritable(-1);
	}
	
	
	
	
	
	
	public SalesKey(IntWritable rid,IntWritable prid,
			IntWritable syear,IntWritable salesmonth){
		this.region_id =  rid;
		this.PromotionID = prid;
		this.sales_month = salesmonth;
		this.sales_year = syear;
	}
	
	public void set(Text txt){
		
		try{
		String values[] = txt.toString().split("|");
		
		region_id.set(Integer.parseInt(values[0].trim()));
		PromotionID.set(Integer.parseInt(values[3].trim()));
		sales_month.set(Integer.parseInt(values[6].trim()));
		sales_year.set(Integer.parseInt(values[7].trim()));
	}
		catch(Exception  ex){
			region_id.set(-1);
			PromotionID.set(-1);
			sales_month.set(-1);
			sales_year.set(-1);
		}
		
		}
	
	
	
	
	

	public void setRegionId(IntWritable rid){
		this.region_id = rid;
	}
	public void setPromotionId(IntWritable pid){
		this.PromotionID = pid;
	}
	public void setSalesMonth(IntWritable sm){
		this.sales_month = sm;
	}
	public void setSalesYear(IntWritable sy){
		this.sales_year = sy;
	}
	
	public IntWritable getRegionId(){
		return this.region_id;
	}
	public IntWritable getPromotionId(){
		return this.PromotionID;
	}
	public IntWritable getSalesMonth(){
		return this.sales_month ;
	}
	public IntWritable getSalesYear(){
		return this.sales_year ;
	}
	
	
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		PromotionID.readFields(in);
		sales_month.readFields(in);
		sales_year.readFields(in);
		region_id.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
		PromotionID.write(out);
		sales_month.write(out);
		sales_year.write(out);
		region_id.write(out);
		
	}

	@Override
	public String toString(){
		return this.PromotionID.toString()+","+this.region_id.toString()+","+this.sales_month.toString()+","+this.sales_year.toString();
	}
	
	
	@Override
	public int compareTo(Object o) {
		
		SalesKey skey =(SalesKey)o;
		
		if(this.toString().equals(skey.toString()))
			return 1;
		return 0;
	}
}
