package udiold;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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

import utils.MyArrayWritable;

public class UDIR extends MapReduceBase implements Reducer<Text,Text,Text,ArrayWritable> {
    private static ArrayWritable dayVals = new MyArrayWritable(Text.class);
    private static Text[] textArray = new Text[0];

    public void reduce(Text k, Iterator<Text> vals, OutputCollector<Text,ArrayWritable> output, Reporter r) throws IOException {
        List<Text> days = new ArrayList<Text>();
        while (vals.hasNext()) {
            days.add(vals.next());
        
        }
        dayVals.set(days.toArray(textArray));

        output.collect(k,dayVals);
    }
}


