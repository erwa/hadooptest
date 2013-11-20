package utils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

public class MyArrayWritable extends ArrayWritable {
	public MyArrayWritable(Class<? extends Writable> c) {
		super(c);
	}
	
	@Override
	public String toString() {
		return StringUtils.join(",", this.toStrings());
	}
}
