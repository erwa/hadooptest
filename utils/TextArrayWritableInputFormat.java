package utils;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class TextArrayWritableInputFormat extends FileInputFormat<Text, ArrayWritable> {

	@Override
	public RecordReader<Text, ArrayWritable> createRecordReader(InputSplit s,
			TaskAttemptContext c) throws IOException, InterruptedException {
		return new TextArrayWritableLineRecordReader(c.getConfiguration());
	}

}
