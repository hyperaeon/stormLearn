package com.netease.learn.wordcount.duplicate.main;

import com.netease.learn.wordcount.duplicate.bolts.WordCounter;
import com.netease.learn.wordcount.duplicate.bolts.WordNormalize;
import com.netease.learn.wordcount.duplicate.spouts.WordReader;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyMain {

	public static void main(String[] args)  throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalize", new WordNormalize()).shuffleGrouping("word-reader");
		builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-normalize", new Fields("word"));
		
		Config config = new Config();
		config.put("wordsFile", args[0]);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Topology", config, builder.createTopology());
		StormSubmitter.submitTopology("Getting-Started-Topology", config, builder.createTopology());
		Thread.sleep(1000);
		cluster.shutdown();
	}
}
