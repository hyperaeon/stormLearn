package com.netease.learn.wordcount.duplicate.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader extends BaseRichSpout{

	private SpoutOutputCollector collector;
	private FileReader reader;
	private boolean completed;
	private TopologyContext context;
	
	private static String FILE_NAME = "wordsFile";
	
	public boolean isDistributed() {
		return false;
	}
	
	public void ack(Object msgId) {
		System.out.println("OK: " + msgId);
	}
	
	public void close() {
		
	}
	
	public void fail(Object msgId) {
		System.out.println("Failed: " + msgId);
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.context = context;
			this.reader = new FileReader(conf.get(FILE_NAME).toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("can't find " + conf.get(FILE_NAME), e);
		}
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				
			}
			return;
		}
		String line;
		BufferedReader bufferedReader = new BufferedReader(reader);
		try {
			while ((line = bufferedReader.readLine()) != null) {
				this.collector.emit(new Values(line), line);
			}
		} catch (Exception e) {
			throw new RuntimeException("read failed ", e);
		} finally {
			completed = true;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}
