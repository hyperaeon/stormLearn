package chapter3;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.io.IOUtils;

public class HadoopRead {

	public static void main(String[] args) throws Exception {
		InputStream in = null;
		try {
			in = new URL("hdfs://user/").openStream();
		} finally {
			IOUtils.closeStream(in);
		}
	}
}
