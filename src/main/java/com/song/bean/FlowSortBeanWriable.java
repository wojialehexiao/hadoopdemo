package com.song.bean;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Duo Nuo on 2018/2/3 0003.
 */
public class FlowSortBeanWriable implements WritableComparable<FlowSortBeanWriable> {



	private Long upFlow;
	private Long dFlow;
	private Long sumFlow;

	public FlowSortBeanWriable() {
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

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(upFlow);
		dataOutput.writeLong(dFlow);
		dataOutput.writeLong(sumFlow);
	}

	public void readFields(DataInput dataInput) throws IOException {
		upFlow = dataInput.readLong();
		dFlow = dataInput.readLong();
		sumFlow = dataInput.readLong();
	}

	@Override
	public String toString() {
		return upFlow + "\t" + dFlow + "\t" + sumFlow;
	}

	public int compareTo(FlowSortBeanWriable flowSortBeanWriable) {
		return flowSortBeanWriable.getSumFlow() >= this.getSumFlow() ? 1 : -1;
	}
}
