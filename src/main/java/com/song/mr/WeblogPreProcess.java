package com.song.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.song.bean.WebLogBean;
import com.song.bean.WebLogParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeblogPreProcess {
	static class WeblogPreProcessMapper
			extends Mapper<LongWritable, Text, Text, NullWritable> {
		Set<String> pages = new HashSet();
		Text k = new Text();
		NullWritable v = NullWritable.get();

		protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			this.pages.add("/about");
			this.pages.add("/black-ip-list/");
			this.pages.add("/cassandra-clustor/");
			this.pages.add("/finance-rhive-repurchase/");
			this.pages.add("/hadoop-family-roadmap/");
			this.pages.add("/hadoop-hive-intro/");
			this.pages.add("/hadoop-zookeeper-intro/");
			this.pages.add("/hadoop-mahout-roadmap/");
		}

		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			WebLogBean webLogBean = WebLogParser.parser(line);

			WebLogParser.filtStaticResource(webLogBean, this.pages);

			this.k.set(webLogBean.toString());
			context.write(this.k, this.v);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(WeblogPreProcess.class);

		job.setMapperClass(WeblogPreProcessMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path[]{new Path("E:\\BaiduNetdiskDownload\\资料文件\\day14\\项目代码\\原始数据\\access.log.fensi")});
		FileOutputFormat.setOutputPath(job, new Path("E:\\BaiduNetdiskDownload\\资料文件\\day14\\项目代码\\原始数据\\18out"));

		job.setNumReduceTasks(0);

		job.waitForCompletion(true);
	}
}
