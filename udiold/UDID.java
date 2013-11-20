package udiold;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UDID {
    public void run(String[] args) throws Exception {
       JobConf conf = new JobConf();

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

       FileInputFormat.setInputPaths(conf, new Path(args[0]));
       FileOutputFormat.setOutputPath(conf, new Path(args[1]));

       JobClient.runJob(conf);
    }

	public static void main(String[] args) throws Exception {
        UDID udi = new UDID();
        udi.run(args);
    }
}
