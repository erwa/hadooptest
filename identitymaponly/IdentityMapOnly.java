package identitymaponly;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IdentityMapOnly extends Configured implements Tool {
	@Override
    public int run(String[] args) throws Exception {
       Job job = new Job();

       job.setJarByClass(getClass());
       job.setJobName(getClass().getCanonicalName());
       
       job.setNumReduceTasks(0);

       FileInputFormat.setInputPaths(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));

       System.out.println("DRIVER OUTPUT");
       
       return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new IdentityMapOnly(), args);
        System.exit(exitCode);
    }
}
