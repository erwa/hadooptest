package udiold;

import azkaban.jobtype.javautils.AbstractHadoopJob;
import azkaban.utils.Props;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class UDIHadoopJavaD extends AbstractHadoopJob {
    
	private final String inputPath;
	private final String outputPath;
	
	public UDIHadoopJavaD(String name, Props props) {
		super(name, props);
		inputPath = props.getString("input.path");
		outputPath = props.getString("output.path");
	}
	
	public void run() throws Exception {
		JobConf conf = getJobConf();

        conf.setJarByClass(UDIM.class);
        conf.setJobName(getClass().getCanonicalName());
        conf.setMapperClass(UDIM.class);
        conf.setReducerClass(UDIR.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
       
        conf.setOutputKeyClass(Text.class);
       
        // This seems to be used for MapOutputValueClass, too, unless you
        // also call setMapOutputValueClass().
        conf.setOutputValueClass(ArrayWritable.class);

        FileInputFormat.addInputPath(conf, new Path(inputPath));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath));
        
        // force output overwrite
        FileSystem fs = FileOutputFormat.getOutputPath(conf).getFileSystem(conf);
        fs.delete(FileOutputFormat.getOutputPath(conf), true);

        super.run();
        
        super.run();
    }
}