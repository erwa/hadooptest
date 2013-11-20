package udinew;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UDIDNew extends Configured implements Tool {
    public int run(String[] args) throws Exception {
       Job job = new Job();

       job.setJarByClass(getClass());
       job.setJobName(getClass().getCanonicalName());
       job.setMapperClass(UDIMNew.class);
       job.setReducerClass(UDIRNew.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(Text.class);
       
       job.setOutputKeyClass(Text.class);
       
       // This seems to be used for MapOutputValueClass, too, unless you
       // also call setMapOutputValueClass().
       job.setOutputValueClass(ArrayWritable.class);

       FileInputFormat.setInputPaths(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));

       System.out.println("DRIVER OUTPUT");
       
       return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UDIDNew(), args);
        System.exit(exitCode);
    }
}