package udinew;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.MyArrayWritable;

public class UDIRNew extends Reducer<Text,Text,Text,ArrayWritable> {
    private ArrayWritable dayVals = new MyArrayWritable(Text.class);
    private Text[] textArray = new Text[0];

    // It's very good practice to add @Override, so you don't accidentally NOT override the reduce function,
    // which causes the default identity reducer to be used.
    @Override
    public void reduce(Text k, Iterable<Text> vals, Context c) throws IOException, InterruptedException {
    	System.out.println("REDUCER OUTPUT");
    	
    	List<Text> days = new ArrayList<Text>();
    	for (Text t : vals) {
            days.add(t);
        }
        dayVals.set(days.toArray(textArray));

        c.write(k,dayVals);
    }
}


