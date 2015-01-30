package customwc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.MyArrayWritable;
import utils.UrlDate;
import utils.UrlDateArrayWritableInputFormat;
import utils.UrlIp;

public class CustomWC extends Configured implements Tool {
    public static class MyMapper extends Mapper<UrlDate,ArrayWritable,UrlIp,Text> {
        private UrlIp urlip = new UrlIp();
        private Text date = new Text();

        @Override
        public void map(UrlDate ud, ArrayWritable aw, Context c) 
        		throws IOException, InterruptedException {
            urlip.setUrl(ud.getUrl());
        	date.set(ud.getDate());

            String[] ips = aw.toStrings();
            for (String ip : ips) {
                urlip.setIp(ip);
                c.write(urlip,date);
            }
        }
    }

    public static class MyReducer extends Reducer<UrlIp,Text,UrlIp,ArrayWritable> {
    	private ArrayWritable days = new MyArrayWritable(Text.class);
        private Text[] textArray = new Text[0];

    	@Override
        public void reduce(UrlIp k, Iterable<Text> vals, Context c)
        		throws IOException, InterruptedException {
            
    		List<Text> dayVals = new ArrayList<Text>();
        	for (Text t : vals) {
        		dayVals.add(t);
            }
            days.set(dayVals.toArray(textArray));
            c.write(k,days);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
    	Configuration c = new Configuration();
    	
    	c.set("mapred.child.java.opts", "-Xmx4096m");
    	
    	Job job = new Job(c);

    	job.setJarByClass(getClass());
    	job.setJobName(getClass().getCanonicalName());
       
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
       
    	job.setInputFormatClass(UrlDateArrayWritableInputFormat.class);
       
    	job.setMapperClass(MyMapper.class);
       
    	job.setMapOutputKeyClass(UrlIp.class);
    	job.setMapOutputValueClass(Text.class);
       
    	job.setReducerClass(MyReducer.class);

    	FileOutputFormat.setOutputPath(job, new Path(args[1]));

    	return job.waitForCompletion(true) ? 0 : 1;
    }

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new CustomWC(), args);
        System.exit(exitCode);
    }
}
