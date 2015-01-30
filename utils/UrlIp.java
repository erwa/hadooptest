package utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class UrlIp implements WritableComparable<UrlIp> {

	private Text url = new Text();
	private Text ip = new Text();
	
	public UrlIp() {}
	
	public UrlIp(String url, String ip) {
		this.url.set(url);
		this.ip.set(ip);
	}
	
	public Text getUrl() {
		return url;
	}
	
	public void setUrl(String url) {
		this.url.set(url);
	}

	public Text getIp() {
		return ip;
	}
	
	public void setIp(String ip) {
		this.ip.set(ip);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		url.readFields(in);
		ip.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		url.write(out);
		ip.write(out);
	}

	@Override
	public int compareTo(UrlIp ui) {
		return this.url.compareTo(ui.getUrl()) != 0 ?
			this.url.compareTo(ui.getUrl()) :
			this.ip.compareTo(ui.getIp());
	}
	
	@Override
	public String toString() {
		return this.url + "," + this.ip;
	}
}
