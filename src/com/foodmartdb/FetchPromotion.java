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

public class FetchPromotion {
	
	public static final String HDFS_ROOT_URL="hdfs://localhost:54310";
	
	
	
	public static List<String> getPromotionID() throws IOException{
	
		String uri = HDFS_ROOT_URL+"/foodmart/promotion.txt";
		List<String> promotionIdList = new ArrayList<String>();
		FileSystem fs = FileSystem.get(URI.create(uri), new Configuration());
			
		   
		   BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
		   String line = bufferedReader.readLine();
		   line = bufferedReader.readLine();
		   String promotionId ="";
		
		   while (line != null) {
			   promotionId = line.split(",")[0].trim();
			   promotionIdList.add(promotionId);
			  System.out.print(promotionId+"  "); 
			  line = bufferedReader.readLine();
		   }
		   System.out.println("--------------------------");
		   System.out.println("Promotion Size : "+promotionIdList.size());
		   System.out.println("--------------------------");
		   
		  return promotionIdList;
	}

}
