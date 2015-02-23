package com.thinklazy.hashtagdiscovery;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;

public class HashtagDiscoveryHelper {
	private static final String datafile = "data/insta.json";
	private static Text keyWord = new Text();
    private static Text valueWord = new Text();
    
	public static void test(String args[]) {	
		//ESDocumentJsonParser.extractHashTags(datafile);
		
		String value = "nike,shopping,walking";
		
		String[] hashs = value.toString().split(",");
        List<String> hashsList = Arrays.asList(hashs);
        
        for (int i = 0; i < hashsList.size(); i++ ) {
        	keyWord.set(hashsList.get(i));
    		
        	//Forwards mapping
        	for (int j = i+1; j < hashsList.size() ; j++ ) {
        		valueWord.set(hashsList.get(j));
        		System.out.println(keyWord.toString() + "=>" + valueWord.toString());
        		//context.write(keyWord, valueWord);
        	}
        }
        
        for (int i = hashsList.size() - 1 ; i >= 0; i-- ) {
        	keyWord.set(hashsList.get(i));
    		
        	//Forwards mapping
        	for (int j = i-1 ; j >= 0; j-- ) {
        		valueWord.set(hashsList.get(j));
        		System.out.println(keyWord.toString() + "=>" + valueWord.toString());
        		//context.write(keyWord, valueWord);
        	}
        }
	}
}
