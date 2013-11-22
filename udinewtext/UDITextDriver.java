package udinewtext;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UDITextDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
       Job job = new Job();

       job.setJarByClass(getClass());
       job.setJobName(getClass().getCanonicalName());
       job.setMapperClass(UDITextMapper.class);
       job.setReducerClass(UDITextReducer.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(Text.class);
       
       job.setOutputKeyClass(Text.class);

       FileInputFormat.setInputPaths(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));

       System.out.println("DRIVER OUTPUT");
       
       return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UDITextDriver(), args);
        System.exit(exitCode);
    }
}