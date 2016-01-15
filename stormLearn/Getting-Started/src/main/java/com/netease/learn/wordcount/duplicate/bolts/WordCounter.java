package com.netease.learn.wordcount.duplicate.bolts;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordCounter extends BaseBasicBolt {

	private Integer id;
	private String name;
	private Map<String, Integer> counter;
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		if (!counter.containsKey(str)) {
			counter.put(str, 1);
		} else {
			counter.put(str, counter.get(str) + 1);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counter = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void cleanup() {
		System.out.println("-- Word count [" + name + "-" + id + " --");
		for (Map.Entry<String, Integer> entrySet : counter.entrySet()) {
			System.out.println(entrySet.getKey() + ":" + entrySet.getValue());
		}
	}

}
