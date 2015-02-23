package com.thinklazy.hashtagdiscovery;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class ESDocumentJsonParser {
	
	public static void main(String args[]) {
		try {
			parseFile("data/instagram_122013_dump.json");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void parseFile(String file) throws FileNotFoundException, UnsupportedEncodingException {
		JSONParser parser = new JSONParser();
		PrintWriter writer = new PrintWriter("data/input", "UTF-8");
		int count = 0 ;
		
		try {
			Object obj = parser.parse(new FileReader(file));
			JSONArray docs = (JSONArray) obj;

			Iterator<JSONObject> docIterator = docs.iterator();
			while (docIterator.hasNext()) {
				JSONObject docObject = docIterator.next();

				System.out.println(count++ +  " : " + (String) docObject.get("_id"));
				JSONObject source = (JSONObject) docObject.get("_source");

				JSONArray hashtags = (JSONArray) source.get("hashtags");

				if(hashtags == null) {
					continue;
				}
				Iterator<String> iterator = hashtags.iterator();
				while (iterator.hasNext()) {
					writer.print(iterator.next() + ",");
				}
				writer.print("\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			writer.close();
		}
	}

	public static List<String> parseHashTags(String line) {
		JSONParser parser = new JSONParser();
		List<String> hashsList = new ArrayList<String>();
		
		try {
			
			Object obj = parser.parse(line);
			JSONObject docObject = (JSONObject) obj;

			System.out.println((String) docObject.get("_index"));
			JSONObject source = (JSONObject) docObject.get("_source");

			JSONArray hastags = (JSONArray) source.get("hashtags");

			Iterator<String> iterator = hastags.iterator();
			while (iterator.hasNext()) {
				hashsList.add(iterator.next());
				//System.out.print(iterator.next() + ",");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return hashsList;
	}
}
