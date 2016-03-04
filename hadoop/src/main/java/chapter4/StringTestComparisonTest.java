package chapter4;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;

public class StringTestComparisonTest {

	public static void main(String[] args ) throws Exception {
//		textTest();
		string();
	}
	
	public static void textTest() {
		Text t = new Text("hadoop");
		System.out.println(t.getLength());
		System.out.println(t.getBytes().length);
		System.out.println(t.charAt(2));
		System.out.println(t.find("do"));
	}
	
	public static void string() throws UnsupportedEncodingException {
		String s = "\u0041\u00DF\u6771\uD801\uDC00";
		System.out.println(s.length());
		System.out.println(s.getBytes("UTF-8").length);
		System.out.println(s.indexOf("\u6771"));
		System.out.println(s.codePointAt(0));
	}
}
