package udiold;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

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
