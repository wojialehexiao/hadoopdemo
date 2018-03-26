package com.song.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class PageViewsBean implements Writable
{
	private String session;
	private String remote_addr;
	private String timestr;
	private String request;
	private int step;
	private String staylong;
	private String referal;
	private String useragent;
	private String bytes_send;
	private String status;

	public void set(String session, String remote_addr, String timestr, String request, int step, String staylong, String referal, String useragent, String bytes_send, String status)
	{
		this.session = session;
		this.remote_addr = remote_addr;
		this.timestr = timestr;
		this.request = request;
		this.step = step;
		this.staylong = staylong;
		this.referal = referal;
		this.useragent = useragent;
		this.bytes_send = bytes_send;
		this.status = status;
	}

	public String getSession() {
		return this.session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String getRemote_addr() {
		return this.remote_addr;
	}

	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}

	public String getTimestr() {
		return this.timestr;
	}

	public void setTimestr(String timestr) {
		this.timestr = timestr;
	}

	public String getRequest() {
		return this.request;
	}

	public void setRequest(String request) {
		this.request = request;
	}

	public int getStep() {
		return this.step;
	}

	public void setStep(int step) {
		this.step = step;
	}

	public String getStaylong() {
		return this.staylong;
	}

	public void setStaylong(String staylong) {
		this.staylong = staylong;
	}

	public String getReferal() {
		return this.referal;
	}

	public void setReferal(String referal) {
		this.referal = referal;
	}

	public String getUseragent() {
		return this.useragent;
	}

	public void setUseragent(String useragent) {
		this.useragent = useragent;
	}

	public String getBytes_send() {
		return this.bytes_send;
	}

	public void setBytes_send(String bytes_send) {
		this.bytes_send = bytes_send;
	}

	public String getStatus() {
		return this.status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public void readFields(DataInput in) throws IOException
	{
		this.session = in.readUTF();
		this.remote_addr = in.readUTF();
		this.timestr = in.readUTF();
		this.request = in.readUTF();
		this.step = in.readInt();
		this.staylong = in.readUTF();
		this.referal = in.readUTF();
		this.useragent = in.readUTF();
		this.bytes_send = in.readUTF();
		this.status = in.readUTF();
	}

	public void write(DataOutput out)
			throws IOException
	{
		out.writeUTF(this.session);
		out.writeUTF(this.remote_addr);
		out.writeUTF(this.timestr);
		out.writeUTF(this.request);
		out.writeInt(this.step);
		out.writeUTF(this.staylong);
		out.writeUTF(this.referal);
		out.writeUTF(this.useragent);
		out.writeUTF(this.bytes_send);
		out.writeUTF(this.status);
	}
}
