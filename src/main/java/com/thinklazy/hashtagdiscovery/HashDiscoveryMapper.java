package com.thinklazy.hashtagdiscovery;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class HashDiscoveryMapper extends
        Mapper<Object, Text, Text, Text> {
 
    private Text keyWord = new Text();
    private Text valueWord = new Text();
    
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
    	
    	String line = value.toString().substring(1);
		
    	
    	//String[] hashs = value.toString().toLowerCase().split(",");
        //List<String> hashsList = Arrays.asList(hashs);
    	
    	List<String> hashsList = ESDocumentJsonParser.parseHashTags(line);
        
        //Forward mapping
        for (int i = 0; i < hashsList.size(); i++ ) {
        	keyWord.set(hashsList.get(i));
    		
        	for (int j = i+1; j < hashsList.size() ; j++ ) {
        		valueWord.set(hashsList.get(j));
        		context.write(keyWord, valueWord);
        	}
        }
        
        //backward mapping
    	for (int i = hashsList.size() - 1 ; i >= 0; i-- ) {
        	keyWord.set(hashsList.get(i));
    		
        	for (int j = i-1 ; j >= 0; j-- ) {
        		valueWord.set(hashsList.get(j));
        		context.write(keyWord, valueWord);
        	}
        }
    }
}