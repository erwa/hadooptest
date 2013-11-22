package identityaw;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TextArrayWritableLineRecordReader extends RecordReader<Text, ArrayWritable> {
	public static final String KEY_VALUE_SEPARATOR =
	    "mapreduce.input.textarraywritablerecordreader.key.value.separator";

	public static final String VALUE_SEPARATOR =
		"mapreduce.input.textarraywritablerecordreader.value.separator";
	
	private final LineRecordReader lineRecordReader;
	
	private String keyValueSeparator;
	private int keyValueSeparatorLength;
	private String valueSeparator;
	
	private String line;
	
	private Text key = new Text();
	private String valueString;
	private String[] values;
	private Text[] textValues;
	private ArrayWritable valueArrayWritable = new ArrayWritable(Text.class);
	
	public TextArrayWritableLineRecordReader(Configuration c) throws IOException {
		lineRecordReader = new LineRecordReader();
	    this.keyValueSeparator = c.get(KEY_VALUE_SEPARATOR, "\t");
	    this.keyValueSeparatorLength = this.keyValueSeparator.length();
	    this.valueSeparator = c.get(VALUE_SEPARATOR, ",");
	}
	
	@Override
	public void initialize(InputSplit s, TaskAttemptContext c) throws IOException {
		lineRecordReader.initialize(s, c);
	}
	
	@Override
	public boolean nextKeyValue() throws IOException {
	    if (lineRecordReader.nextKeyValue()) {
	    	line = lineRecordReader.getCurrentValue().toString();
	    } else {
	    	return false;
	    }
	    
	    int keyValueSeparatorPos = line.indexOf(this.keyValueSeparator);
	    if (keyValueSeparatorPos == -1) {
	    	key.set(line);
	    	valueArrayWritable.set(new Text[0]);
	    } else {
	    	key.set(line.substring(0, keyValueSeparatorPos));
	    	
	    	valueString = line.substring(keyValueSeparatorPos + keyValueSeparatorLength);
	    	values = valueString.split(valueSeparator);
	    	textValues = new Text[values.length];
	    	for (int i = 0; i < values.length; i++) {
	    		textValues[i] = new Text(values[i]);
	    	}
	    	valueArrayWritable.set(textValues);
	    }
	    
	    return true;
	}
	
	@Override
	public Text getCurrentKey() {
	    return key;
	}

	@Override
	public ArrayWritable getCurrentValue() {
	    return valueArrayWritable;
	}

	@Override
	public float getProgress() {
	    return lineRecordReader.getProgress();
	}

	@Override
	public void close() throws IOException {
	    lineRecordReader.close();
	}
}
