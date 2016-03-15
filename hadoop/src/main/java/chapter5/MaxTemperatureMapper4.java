package chapter5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper4 extends Mapper<LongWritable, Text, Text, IntWritable> {

	enum Temperature {
		OVER_100
	}
	
	private NcdcRecordParser parser = new NcdcRecordParser();
	
	public void main(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		parser.parse(value);
		if (parser.isValidTemperature()) {
			int airTemperature = parser.getAirTemperature();
			if (airTemperature > 1000) {
				System.err.println("Temperature over 100 degrees for input: " + value);
				context.setStatus("Detectd possibly corrupt records: see logs");
				context.getCounter(Temperature.OVER_100);
			}
			context.write(new Text(parser.getYear()), new IntWritable(airTemperature));
		}
	}
}
