package utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class UrlDateArrayWritableLineRecordReader extends RecordReader<UrlDate, ArrayWritable> {
	public static final String KEY_VALUE_SEPARATOR =
	    "mapreduce.input.urldatearraywritablerecordreader.key.value.separator";

	public static final String KEY_SEPARATOR =
		"mapreduce.input.urldatearraywritablerecordreader.key.separator";
	
	public static final String VALUE_SEPARATOR =
		"mapreduce.input.urldatearraywritablerecordreader.value.separator";
	
	private final LineRecordReader lineRecordReader;
	
	private String keySeparator;
	private String keyValueSeparator;
	private String valueSeparator;
	
	private String line;
	
	private UrlDate key = new UrlDate();
	private String[] keyValue;
	private String[] keyParts;
	private String[] values;
	private Text[] textValues;
	private ArrayWritable valueArrayWritable = new ArrayWritable(Text.class);
	
	public UrlDateArrayWritableLineRecordReader(Configuration c) throws IOException {
		lineRecordReader = new LineRecordReader();
		this.keySeparator = c.get(KEY_SEPARATOR, ",");
	    this.keyValueSeparator = c.get(KEY_VALUE_SEPARATOR, "\t");
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
	    
	    keyValue = line.split(this.keyValueSeparator);
	    keyParts = keyValue[0].split(this.keySeparator);
    	key.setUrl(keyParts[0]);
    	key.setDate(keyParts[1]);
    	
    	values = keyValue[1].split(valueSeparator);
    	textValues = new Text[values.length];
    	for (int i = 0; i < values.length; i++) {
    		textValues[i] = new Text(values[i]);
    	}
    	valueArrayWritable.set(textValues);
	    
	    return true;
	}
	
	@Override
	public UrlDate getCurrentKey() {
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
