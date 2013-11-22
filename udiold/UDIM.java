package udiold;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class UDIM extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {
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
