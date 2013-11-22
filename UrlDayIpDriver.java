//package test;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class UrlDayIpDriver {
    public void run(String[] args) throws Exception {
       JobConf conf = new JobConf();

       conf.setJarByClass(UrlDayIpMapper.class);
       conf.setJobName(getClass().getCanonicalName());
       conf.setMapperClass(UrlDayIpMapper.class);
       conf.setReducerClass(UrlDayIpReducer.class);

       conf.setMapOutputKeyClass(Text.class);
       conf.setOutputKeyClass(Text.class);

       FileInputFormat.setInputPaths(conf, new Path(args[0]));
       FileOutputFormat.setOutputPath(conf, new Path(args[1]));

       JobClient.runJob(conf);
    }

	public static void main(String[] args) throws Exception {
        UrlDayIpDriver udi = new UrlDayIpDriver();
        udi.run(args);
    }
}
