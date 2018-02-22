package com.song.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Duo Nuo on 2018/2/3 0003.
 */
public class FlowBeanWritable implements Writable {

	Long upFlow;
	Long dFlow;
	Long sumFlow;

	public FlowBeanWritable() {
	}

	public FlowBeanWritable(Long upFlow, Long dFlow) {
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = upFlow + dFlow;
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(upFlow);
		dataOutput.writeLong(dFlow);
	}

	public void readFields(DataInput dataInput) throws IOException {
		upFlow = dataInput.readLong();
		dFlow = dataInput.readLong();
	}

	public Long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(Long upFlow) {
		this.upFlow = upFlow;
	}

	public Long getdFlow() {
		return dFlow;
	}

	public void setdFlow(Long dFlow) {
		this.dFlow = dFlow;
	}

	public Long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(Long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public String toString() {
		sumFlow = upFlow + dFlow;
		return upFlow + "\t" + dFlow + "\t" + sumFlow;
	}
}
