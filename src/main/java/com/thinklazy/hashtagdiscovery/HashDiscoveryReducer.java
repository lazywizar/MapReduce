package com.thinklazy.hashtagdiscovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HashDiscoveryReducer extends Reducer<Text, Text, Text, Text> {
	private Text allCoOccuringWords = new Text();
    private static final Long THRES_HOLD = 1L;
	
	public void reduce(Text text, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Map<String, Long> coOccuranceFreq = new HashMap<String, Long>();
		for (Text value : values) {
			if (coOccuranceFreq.containsKey(value.toString())) {
				Long currentFreq = coOccuranceFreq.get(value.toString());
				coOccuranceFreq.put(value.toString(), currentFreq+1);
			} else {
				coOccuranceFreq.put(value.toString(), new Long(1));
			}
		}
		List<Entry<String, Long>> list = entriesSortedByValues(coOccuranceFreq);
		
		List<Entry<String, Long>> threasholdList = new ArrayList<Entry<String, Long>>();
		
		for(Entry<String, Long> entry : list) {
			if(entry.getValue() > THRES_HOLD) {
				threasholdList.add(entry);
			}
		}
		if(threasholdList.size() > 0) {
			allCoOccuringWords.set(threasholdList.toString());
			context.write(text, allCoOccuringWords);
		}
	}

	static <K, V extends Comparable<? super V>> List<Entry<K, V>> entriesSortedByValues(
			Map<K, V> map) {

		List<Entry<K, V>> sortedEntries = new ArrayList<Entry<K, V>>(
				map.entrySet());

		Collections.sort(sortedEntries, new Comparator<Entry<K, V>>() {
			@Override
			public int compare(Entry<K, V> e1, Entry<K, V> e2) {
				return e2.getValue().compareTo(e1.getValue());
			}
		});

		return sortedEntries;
	}
}
