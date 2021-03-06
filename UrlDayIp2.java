//package test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
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

public class UrlDayIp2 {
    public static class MyMap extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {
        private Text urlip = new Text();
        private Text day = new Text();

        public void map(LongWritable k, Text v, OutputCollector<Text,Text> output, Reporter reporter) throws IOException {
            String line = v.toString();
            String[] words = line.split(",");
            String url = words[0];
            
            day.set(words[1]);

            for (int i = 2; i < words.length; i++) {
                urlip.set(url + "," + words[i]);
                output.collect(urlip,day); 
            }
        }
    }

    public static class MyReduce extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
       private Text daysVal = new Text(); 

        public void reduce(Text k, Iterator<Text> vals, OutputCollector<Text,Text> output, Reporter r) throws IOException {
            String days = vals.next().toString();
            while (vals.hasNext()) {
                days += ","+vals.next().toString();
            
            }
            daysVal.set(days);
            output.collect(k,daysVal);
        }
    }


    public void run(String[] args) throws Exception {
    }

	public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(UrlDayIp2.class);

       conf.setJobName("UrlDayIp2");
       conf.setMapperClass(UrlDayIp2.MyMap.class);
       conf.setReducerClass(UrlDayIp2.MyReduce.class);
       FileInputFormat.setInputPaths(conf, new Path(args[0]));
       FileOutputFormat.setOutputPath(conf, new Path(args[1]));

       JobClient.runJob(conf);
    }
}
