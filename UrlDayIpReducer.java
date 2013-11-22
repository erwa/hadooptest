//package test;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;

import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class UrlDayIpReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
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


