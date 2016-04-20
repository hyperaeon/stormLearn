package spark.chapter3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;

public class AnonyFunction {

	public static void main(String[] args) {
		String file = "";
		SparkConf conf = new SparkConf().setAppName("app");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(file);
		JavaRDD<String> errors = lines.filter(new Function<String, Boolean>(){
			public Boolean call(String x) {
				return x.contains("error");
			}
		});
	}
}
