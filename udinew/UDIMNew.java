package udinew;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UDIMNew extends Mapper<LongWritable,Text,Text,Text> {
    private Text urlip = new Text();
    private Text day = new Text();

    // It's very good practice to add @Override, so you don't accidentally NOT override the map function,
    // which causes the default identity mapper to be used.
    @Override
    public void map(LongWritable k, Text v, Context c) throws IOException, InterruptedException {
    	System.out.println("MAPPER OUTPUT");
    	
        String line = v.toString();
        String[] words = line.split(",");
        String url = words[0];
        
        day.set(words[1]);

        for (int i = 2; i < words.length; i++) {
            urlip.set(url + "," + words[i]);
            c.write(urlip,day); 
        }
    }
}
