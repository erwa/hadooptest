package customwc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UrlDate implements WritableComparable<UrlDate> {

	String url;
	String date;
	
	public UrlDate(String url, String date) {
		this.url = url;
		this.date = date;
	}
	
	public String getUrl() {
		return url;
	}

	public String getDate() {
		return date;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		byte[] bytes = new byte[length];
		in.readFully(bytes, 0, length);
		url = bytes.toString();
		length = in.readInt();
		bytes = new byte[length];
		in.readFully(bytes, 0, length);
		date = bytes.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.write(url.length());
		out.writeChars(url);
		out.write(date.length());
		out.writeChars(date);
	}

	@Override
	public int compareTo(UrlDate ud) {
		return this.url.compareTo(ud.getUrl()) != 0 ? 
				this.url.compareTo(ud.getUrl()) :
				this.date.compareTo(ud.getDate());
	}
	
}
