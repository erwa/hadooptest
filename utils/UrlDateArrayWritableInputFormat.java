package utils;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class UrlDateArrayWritableInputFormat extends FileInputFormat<UrlDate, ArrayWritable> {

	@Override
	public RecordReader<UrlDate, ArrayWritable> createRecordReader(InputSplit s,
			TaskAttemptContext c) throws IOException, InterruptedException {
		return new UrlDateArrayWritableLineRecordReader(c.getConfiguration());
	}

}
