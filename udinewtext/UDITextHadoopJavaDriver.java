package udinewtext;

import azkaban.jobtype.javautils.AbstractHadoopJob;
import azkaban.utils.Props;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class UDITextHadoopJavaDriver extends AbstractHadoopJob {
    
	private final String inputPath;
	private final String outputPath;
	
	public UDITextHadoopJavaDriver(String name, Props props) {
		super(name, props);
		inputPath = props.getString("input.path");
		outputPath = props.getString("output.path");
	}
	
	public int run(String[] args) throws Exception {
       Job job = new Job();

       job.setJarByClass(getClass());
       job.setJobName(getClass().getCanonicalName());
       job.setMapperClass(UDITextMapper.class);
       job.setReducerClass(UDITextReducer.class);

       job.setMapOutputKeyClass(Text.class);
       job.setMapOutputValueClass(Text.class);
       
       job.setOutputKeyClass(Text.class);

       FileInputFormat.setInputPaths(job, new Path(inputPath));
       FileOutputFormat.setOutputPath(job, new Path(outputPath));
       
       // Force output overwrite
       FileSystem fs = FileOutputFormat.getOutputPath(job).getFileSystem(job.getConfiguration());
       fs.delete(FileOutputFormat.getOutputPath(job), true);

       System.out.println("DRIVER OUTPUT");
       
       return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new UDITextDriver(), args);
        System.exit(exitCode);
    }
}