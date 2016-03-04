package chapter5;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationTest {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		InputStream is = new FileInputStream(new File("/home/hadoop/git/hadoop/src/main/java/chapter5/configuration-1.xml"));
		conf.addResource(is);
		InputStream is2 = new FileInputStream(new File("/home/hadoop/git/hadoop/src/main/java/chapter5/configuration-2.xml"));
		conf.addResource(is2);
		System.out.println(conf.get("color"));
		System.out.println(conf.getInt("size", 0));
		System.out.println(conf.get("breadth", "wide"));
		
	}
}
