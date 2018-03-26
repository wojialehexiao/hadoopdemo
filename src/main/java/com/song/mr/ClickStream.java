package com.song.mr;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.UUID;

import com.song.bean.WebLogBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ClickStream {
	static class ClickStreamMapper
			extends Mapper<LongWritable, Text, Text, WebLogBean> {
		Text k = new Text();
		WebLogBean v = new WebLogBean();

		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, WebLogBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();

			String[] fields = line.split("\001");
			if (fields.length < 9) {
				return;
			}
			this.v.set("true".equals(fields[0]), fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8]);

			if (this.v.isValid()) {
				this.k.set(this.v.getRemote_addr());
				context.write(this.k, this.v);
			}
		}
	}

	static class ClickStreamReducer extends Reducer<Text, WebLogBean, NullWritable, Text> {
		Text v = new Text();

		protected void reduce(Text key, Iterable<WebLogBean> values, Reducer<Text, WebLogBean, NullWritable, Text>.Context context) throws IOException, InterruptedException {
			ArrayList<WebLogBean> beans = new ArrayList();

			try {
				for (WebLogBean bean : values) {
					WebLogBean webLogBean = new WebLogBean();
					try {
						BeanUtils.copyProperties(webLogBean, bean);
					} catch (Exception e) {
						e.printStackTrace();
					}
					beans.add(webLogBean);
				}

				Collections.sort(beans, new Comparator<WebLogBean>() {

					public int compare(WebLogBean o1, WebLogBean o2) {
						try {
							Date d1 = ClickStream.ClickStreamReducer.this.toDate(o1.getTime_local());
							Date d2 = ClickStream.ClickStreamReducer.this.toDate(o2.getTime_local());
							if ((d1 == null) || (d2 == null))
								return 0;
							return d1.compareTo(d2);
						} catch (Exception e) {
							e.printStackTrace();
						}
						return 0;
					}

				});
				int step = 1;
				String session = UUID.randomUUID().toString();
				for (int i = 0; i < beans.size(); i++) {
					WebLogBean bean = (WebLogBean) beans.get(i);

					if (1 == beans.size()) {
						this.v.set(session + "\001" + key.toString() + "\001" + bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + 60 + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001" +
								bean.getStatus());
						context.write(NullWritable.get(), this.v);
						session = UUID.randomUUID().toString();
						break;
					}

					if (i != 0) {
						long timeDiff = timeDiff(toDate(bean.getTime_local()), toDate(((WebLogBean) beans.get(i - 1)).getTime_local()));

						if (timeDiff < 1800000L) {
							this.v.set(session + "\001" + key.toString() + "\001" + ((WebLogBean) beans.get(i - 1)).getRemote_user() + "\001" + ((WebLogBean) beans.get(i - 1)).getTime_local() + "\001" + ((WebLogBean) beans.get(i - 1)).getRequest() + "\001" + step + "\001" + timeDiff / 1000L + "\001" + ((WebLogBean) beans.get(i - 1)).getHttp_referer() + "\001" +
									((WebLogBean) beans.get(i - 1)).getHttp_user_agent() + "\001" + ((WebLogBean) beans.get(i - 1)).getBody_bytes_sent() + "\001" + ((WebLogBean) beans.get(i - 1)).getStatus());
							context.write(NullWritable.get(), this.v);
							step++;
						} else {
							this.v.set(session + "\001" + key.toString() + "\001" + ((WebLogBean) beans.get(i - 1)).getRemote_user() + "\001" + ((WebLogBean) beans.get(i - 1)).getTime_local() + "\001" + ((WebLogBean) beans.get(i - 1)).getRequest() + "\001" + step + "\001" + 60 + "\001" + ((WebLogBean) beans.get(i - 1)).getHttp_referer() + "\001" +
									((WebLogBean) beans.get(i - 1)).getHttp_user_agent() + "\001" + ((WebLogBean) beans.get(i - 1)).getBody_bytes_sent() + "\001" + ((WebLogBean) beans.get(i - 1)).getStatus());
							context.write(NullWritable.get(), this.v);

							step = 1;
							session = UUID.randomUUID().toString();
						}

						if (i == beans.size() - 1) {
							this.v.set(session + "\001" + key.toString() + "\001" + bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + 60 + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001" + bean.getStatus());
							context.write(NullWritable.get(), this.v);
						}
					}
				}
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		private String toStr(Date date) {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			return df.format(date);
		}

		private Date toDate(String timeStr) throws ParseException {
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
			return df.parse(timeStr);
		}

		private long timeDiff(String time1, String time2) throws ParseException {
			Date d1 = toDate(time1);
			Date d2 = toDate(time2);
			return d1.getTime() - d2.getTime();
		}

		private long timeDiff(Date time1, Date time2)
				throws ParseException {
			return time1.getTime() - time2.getTime();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		System.setProperty("user.name","song");
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Job job = Job.getInstance(conf);

		job.setJarByClass(ClickStream.class);

		job.setMapperClass(ClickStreamMapper.class);
		job.setReducerClass(ClickStreamReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WebLogBean.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path[]{new Path("E:\\BaiduNetdiskDownload\\资料文件\\day14\\生成日志数据的python程序\\mylog.log")});
		FileOutputFormat.setOutputPath(job, new Path("E:\\BaiduNetdiskDownload\\资料文件\\day14\\生成日志数据的python程序\\pageviews"));

		job.waitForCompletion(true);
	}
}
