package udinew;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import utils.MyArrayWritable;

public class UDIRNew extends Reducer<Text,Text,Text,ArrayWritable> {
    private static ArrayWritable dayVals = new MyArrayWritable(Text.class);
    private static Text[] textArray = new Text[0];

    public void reduce(Text k, Iterator<Text> vals, Context c) throws IOException, InterruptedException {
    	System.out.println("REDUCER OUTPUT");
    	
    	List<Text> days = new ArrayList<Text>();
        while (vals.hasNext()) {
            days.add(vals.next());
        }
        dayVals.set(days.toArray(textArray));

        c.write(k,dayVals);
    }
}


