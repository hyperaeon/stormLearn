package chapter1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class LoadBalance {

	private static Map<String,Integer> serverWeightMap = new HashMap<String, Integer>();
	
	private static Integer pos = 0;
	
	static {

		serverWeightMap.put("192.168.1.100", 1);
		serverWeightMap.put("192.168.1.101", 1);
		serverWeightMap.put("192.168.1.102", 4);
		
		serverWeightMap.put("192.168.1.103", 1);
		serverWeightMap.put("192.168.1.104", 1);
		
		serverWeightMap.put("192.168.1.105", 3);
		serverWeightMap.put("192.168.1.106", 1);
		serverWeightMap.put("192.168.1.107", 2);
		serverWeightMap.put("192.168.1.108", 1);
		
		serverWeightMap.put("192.168.1.109", 1);
		serverWeightMap.put("192.168.1.110", 1);
	}
	
	public static String testRoundRobin() {
		Map<String, Integer> serverMap = new HashMap<String,Integer>();
		serverMap.putAll(serverWeightMap);
		
		Set<String> keySet = serverMap.keySet();
		ArrayList<String> keyList=  new ArrayList<String>();
		keyList.addAll(keySet);
		
		String server = null;
		synchronized (pos) {
			if (pos >= keySet.size() ){
				pos = 0;
			}
			server = keyList.get(pos);
			pos ++;
		}
		return server;
	}
	
	public static void main(String[] args) {
		for (int i = 0; i < 30; i++) {
			System.out.println(testRoundRobin());
		}
	}
}
