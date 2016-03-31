//package chapter13;
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.Mapper;
//import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Reporter;
//import org.apache.hadoop.mapred.lib.NullOutputFormat;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import chapter5.NcdcRecordParser;
//
//public class HBaseTemperatureImporter extends Configured implements Tool {
//
//	static class HBaseTemperatureMapper<K, V> extends MapReduceBase implements Mapper<LongWritable, Text, K, V> {
//		
//		private NcdcRecordParser parser = new NcdcRecordParser();
//		private HTable table;
//		
//		public void map(LongWritable key, Text value, OutputCollector<K, V> output, Reporter reporter) throws IOException {
//			parser.parse(value.toString());
//			if (parser.isValidTemperature()) {
//				byte[] rowKey = RowKeyConverter.makeObservationRowkey(parser.getStationId()), parser.getObeservationDate().getTime());
//				Put p = new Put(rowKey);
//				p.add(HBaseTemperatureCli.AIRTEMP_QUALIFIER, Bytes.toBytes(parser.getAirTemperature()));
//				table.put(p);
//			}
//		}
//	}
//	
//	public void configure(JobConf jc) {
//		super.configure(jc);
//		try {
//			this.table = new HTable(new HBaseConfiguration(jc), "observations");
//		} catch (IOException e) {
//			throw new RuntimeException ("Failed HTable construction", e);
//		}
//	}
//	
//	@Override
//	public void close() throws IOException {
//		super.close();
//		table.close();
//	}
//	
//	public int run(String[] args) throws Exception {
//		if (args.length != 1) {
//			System.err.println("Usage: HBaseTemperatureImporter <input>");
//			return -1;
//		}
//		JobConf jc = new JobConf(getConf(), getClass());
//		FileInputFormat.addInputPath(jc, new Path(args[0]));
//		jc.setMapperClass(HBaseTemperatureMapper.class);
//		jc.setNumReduceTasks(0);
//		jc.setOutputFormat(NullOutputFormat.class);
//		JobClient.runJob(jc);
//		return 0;
//	}
//
//	public static void main(String[] args) throws Exception {
//		int exitCode = ToolRunner.run(new HBaseConfiguration(), new HBaseTemperatureImporter(), args);
//		System.exit(exitCode);
//	}
//}
