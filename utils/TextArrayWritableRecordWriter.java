package utils;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;

public class TextArrayWritableRecordWriter extends RecordWriter<Text, ArrayWritable> {
	public static final String KEY_VALUE_SEPARATOR =
		    "mapreduce.output.textarraywritablerecordwriter.key.value.separator";

	public static final String VALUE_SEPARATOR =
			"mapreduce.output.textarraywritablerecordwriter.value.separator";

	private DataOutputStream out;
	
	private String keyValueSeparator;
	private String valueSeparator;
	
	public TextArrayWritableRecordWriter(DataOutputStream out, Configuration c) {
		this.out = out;
		
		this.keyValueSeparator = c.get(KEY_VALUE_SEPARATOR, "\t");
	    this.valueSeparator = c.get(VALUE_SEPARATOR, ",");
	}
	
	@Override
	public void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {
		out.close();
	}

	@Override
	public synchronized void write(Text t, ArrayWritable aw) throws IOException,
			InterruptedException {
		System.out.println("WRITING " + t);
		
		t.write(out);
		out.writeChars(keyValueSeparator);
		out.writeChars(StringUtils.join(valueSeparator, aw.toStrings()));
		out.writeByte('\n');
	}

}
