package spark.chapter5;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.codehaus.jackson.map.ObjectMapper;

public class ReadJson {

	public static class A {

	}

	class B {

	}

	public static void main(String[] args) {
		A a = new A();
		B b = new ReadJson().new B();
		SparkConf conf = new SparkConf().setAppName("readJson");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> input = sc.textFile("file.json");
		JavaRDD<Person> result = input.mapPartitions(new ParseJson());
	}
}

class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
	public Iterable<Person> call(Iterator<String> lines) throws Exception {
		ArrayList<Person> people = new ArrayList<Person>();
		ObjectMapper mapper = new ObjectMapper();
		while (lines.hasNext()) {
			String line = lines.next();
			try {
				people.add(mapper.readValue(line, Person.class));
			} catch (Exception e) {

			}
		}
		return people;
	}
}