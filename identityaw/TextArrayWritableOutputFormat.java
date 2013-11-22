package identityaw;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TextArrayWritableOutputFormat extends FileOutputFormat<Text, ArrayWritable> {

	@Override
	public RecordWriter<Text, ArrayWritable> getRecordWriter(TaskAttemptContext c)
			throws IOException, InterruptedException {
		Configuration conf = c.getConfiguration();
		
		Path file = getDefaultWorkFile(c, "");
	    FileSystem fs = file.getFileSystem(conf);
	    FSDataOutputStream fileOut = fs.create(file, false);
	    
		return new TextArrayWritableRecordWriter(fileOut, conf);
	}
}
