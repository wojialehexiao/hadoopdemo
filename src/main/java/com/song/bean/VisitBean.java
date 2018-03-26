package com.song.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class VisitBean implements Writable
{
  private String session;
  private String remote_addr;
  private String inTime;
  private String outTime;
  private String inPage;
  private String outPage;
  private String referal;
  private int pageVisits;
  
  public void set(String session, String remote_addr, String inTime, String outTime, String inPage, String outPage, String referal, int pageVisits)
  {
    this.session = session;
    this.remote_addr = remote_addr;
    this.inTime = inTime;
    this.outTime = outTime;
    this.inPage = inPage;
    this.outPage = outPage;
    this.referal = referal;
    this.pageVisits = pageVisits;
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
  
  public String getInTime() {
    return this.inTime;
  }
  
  public void setInTime(String inTime) {
    this.inTime = inTime;
  }
  
  public String getOutTime() {
    return this.outTime;
  }
  
  public void setOutTime(String outTime) {
    this.outTime = outTime;
  }
  
  public String getInPage() {
    return this.inPage;
  }
  
  public void setInPage(String inPage) {
    this.inPage = inPage;
  }
  
  public String getOutPage() {
    return this.outPage;
  }
  
  public void setOutPage(String outPage) {
    this.outPage = outPage;
  }
  
  public String getReferal() {
    return this.referal;
  }
  
  public void setReferal(String referal) {
    this.referal = referal;
  }
  
  public int getPageVisits() {
    return this.pageVisits;
  }
  
  public void setPageVisits(int pageVisits) {
    this.pageVisits = pageVisits;
  }
  
  public void readFields(DataInput in) throws IOException
  {
    this.session = in.readUTF();
    this.remote_addr = in.readUTF();
    this.inTime = in.readUTF();
    this.outTime = in.readUTF();
    this.inPage = in.readUTF();
    this.outPage = in.readUTF();
    this.referal = in.readUTF();
    this.pageVisits = in.readInt();
  }
  
  public void write(DataOutput out)
    throws IOException
  {
    out.writeUTF(this.session);
    out.writeUTF(this.remote_addr);
    out.writeUTF(this.inTime);
    out.writeUTF(this.outTime);
    out.writeUTF(this.inPage);
    out.writeUTF(this.outPage);
    out.writeUTF(this.referal);
    out.writeInt(this.pageVisits);
  }
  
  public String toString()
  {
    return this.session + "\001" + this.remote_addr + "\001" + this.inTime + "\001" + this.outTime + "\001" + this.inPage + "\001" + this.outPage + "\001" + this.referal + "\001" + this.pageVisits;
  }
}
