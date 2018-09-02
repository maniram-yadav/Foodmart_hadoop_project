package com.foodmartdb;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Mapper;

public class FoodmartMapper 
extends Mapper<SalesKey, SalesValue, SalesKey, SalesValue> {
	
	static List<String>  promotionIDList = new ArrayList<String>();
			static {
				try {
					promotionIDList = FetchPromotion.getPromotionID();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
	
	public void map(SalesKey key, SalesValue value,Context context) throws IOException,InterruptedException
		{
		
		   if(promotionIDList.contains(key.getPromotionId().toString())){
			
			   context.write(key, value);
		   }
		}


}
