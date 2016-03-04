package chapter4;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;

public class IntWritableTest {

	static RawComparator<IntWritable> comparator = WritableComparator.get(IntWritable.class);
	
	public static void main(String[] args) throws IOException {
		IntWritable writable = new IntWritable();
		writable.set(163);
		byte[] bytes = serialize(writable);
		System.out.println(bytes.length);
		IntWritable newWritable = new IntWritable();
		deserialize(newWritable, bytes);
		System.out.println(newWritable.get());
		
		compare(163, 67);
		compare2(67, 163);
	}
	
	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}
	
	public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		return bytes;
	}
	
	public static void compare(Integer i, Integer j ) {
		IntWritable w1 = new IntWritable(i);
		IntWritable w2 = new IntWritable(j);
		System.out.println(comparator.compare(w1, w2));
	}
	
	public static void compare2(Integer i, Integer j) throws IOException {
		IntWritable w1 = new IntWritable(i);
		IntWritable w2 = new IntWritable(j);
		byte[] b1 = serialize(w1);
		byte[] b2 = serialize(w2);
		System.out.println(comparator.compare(b1, 0, b1.length, b2, 0, b2.length));
	}
}
