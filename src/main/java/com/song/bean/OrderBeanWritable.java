package com.song.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Duo Nuo on 2018/2/8 0008.
 */
public class OrderBeanWritable implements Writable{

	private Integer id;

	private String date;

	private String pid;

	private Integer amount;

	private String pname;

	private Integer categoryId;

	private Double price;

	public OrderBeanWritable() {
	}

	public void set(Integer id, String date, String pid, Integer amount, String pname, Integer categoryId, Double price) {
		this.id = id;
		this.date = date;
		this.pid = pid;
		this.amount = amount;
		this.pname = pname;
		this.categoryId = categoryId;
		this.price = price;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public Integer getAmount() {
		return amount;
	}

	public void setAmount(Integer amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public Integer getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(Integer categoryId) {
		this.categoryId = categoryId;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(id);
		dataOutput.writeUTF(date);
		dataOutput.writeUTF(pid);
		dataOutput.writeInt(amount);
		dataOutput.writeUTF(pname);
		dataOutput.writeInt(categoryId);
		dataOutput.writeDouble(price);
	}

	public void readFields(DataInput dataInput) throws IOException {
		id = dataInput.readInt();
		date = dataInput.readUTF();
		pid = dataInput.readUTF();
		amount = dataInput.readInt();
		pname = dataInput.readUTF();
		categoryId = dataInput.readInt();
		price = dataInput.readDouble();
	}

	@Override
	public String toString() {
		return id + "\t" + date + "\t" + pid + "\t" + amount + "\t" + pname + "\t" + categoryId + "\t" + price;
	}
}
