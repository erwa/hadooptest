package identityaw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IdentityAW extends Configured implements Tool {
	@Override
    public int run(String[] args) throws Exception {
		Configuration c = new Configuration();
    	
    	// Set separators for input and output.
		c.set("mapreduce.input.textarraywritablerecordreader.key.value.separator", "SEPARATOR");
    	c.set("mapreduce.input.textarraywritablerecordreader.value.separator", ",");
    	
    	c.set("mapreduce.output.textarraywritablerecordwriter.key.value.separator", "HADOOP");
    	c.set("mapreduce.output.textarraywritablerecordwriter.value.separator", "MAPREDUCE");
    	
    	Job job = new Job(c);

		job.setJarByClass(getClass());
		job.setJobName(getClass().getCanonicalName());

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextArrayWritableInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(ArrayWritable.class);
		
		job.setOutputFormatClass(TextArrayWritableOutputFormat.class);
		
		job.setNumReduceTasks(0);
       
   		return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new IdentityAW(), args);
        System.exit(exitCode);
    }
}