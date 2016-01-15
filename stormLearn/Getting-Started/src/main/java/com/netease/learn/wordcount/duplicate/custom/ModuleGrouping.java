package com.netease.learn.wordcount.duplicate.custom;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.task.WorkerTopologyContext;

public class ModuleGrouping implements CustomStreamGrouping,  Serializable {

	int numTasks = 0;
	
	@Override
	public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
		numTasks = targetTasks.size();
	}

	@Override
	public List<Integer> chooseTasks(int taskId, List<Object> values) {
		List<Integer> boltIds = new ArrayList<Integer>();
		if (values.size() > 0) {
			String str = values.get(0).toString();
			if (str.isEmpty()) {
				boltIds.add(0);
			} else {
				boltIds.add(str.charAt(0) % numTasks);
			}
		}
		return boltIds;
	}

}
