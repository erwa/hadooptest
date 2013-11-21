package maponly;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapOnly extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
    	Configuration c = new Configuration();
    	c.set("mapred.textoutputformat.separator","SEPARATOR");
    	
    	Job job = new Job(c);

    	job.setJarByClass(getClass());
    	job.setJobName(getClass().getCanonicalName());
       
    	FileInputFormat.setInputPaths(job, new Path(args[0]));       
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
    	job.setInputFormatClass(KeyValueTextInputFormat.class);
       
    	job.setOutputKeyClass(Text.class);
       
    	job.setNumReduceTasks(0);
    	return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MapOnly(), args);
        System.exit(exitCode);
    }
}
