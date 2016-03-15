package chapter5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class MaxTemperatureDriverTest {

	@Test
	public void test() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		Path input = new Path("input/ncdc/micro");
		Path output = new Path("output");
		
		FileSystem fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
		MaxTemperatureDriver driver = new MaxTemperatureDriver();
		driver.setConf(conf);
		
		int exitCode = driver.run(new String[]{
				input.toString(), output.toString()
		});
		System.out.println(exitCode);
	}
}
