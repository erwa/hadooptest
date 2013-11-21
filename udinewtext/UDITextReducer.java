package udinewtext;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UDITextReducer extends Reducer<Text,Text,Text,Text> {
	private static Text daysVal = new Text(); 

   	@Override
   	public void reduce(Text k, Iterable<Text> vals, Context c) throws IOException, InterruptedException {
   		System.out.println("REDUCER OUTPUT");
	   
   		Iterator<Text> i = vals.iterator();
   		
   		String days = i.next().toString();
   		while (i.hasNext()) {
   			days += "," + i.next().toString();
   		}
   		daysVal.set(days);
   		c.write(k,daysVal);
   	}
}


