package com.song.mr;

import com.song.bean.FlowSortBeanWriable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Duo Nuo on 2018/2/3 0003.
 */
public class FlowCountSort {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(FlowCountSort.class);

		job.setMapperClass(FlowCountSortMapper.class);
		job.setMapOutputKeyClass(FlowSortBeanWriable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(FlowCountSortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowSortBeanWriable.class);

		FileInputFormat.setInputPaths(job,args[0]);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}

	public static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowSortBeanWriable, Text>{

		FlowSortBeanWriable flowSortBeanWriable = new FlowSortBeanWriable();
		Text text = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] split = line.split("\t");
			String phoneNo = split[0];
			flowSortBeanWriable.setUpFlow(Long.parseLong(split[1]));
			flowSortBeanWriable.setdFlow(Long.parseLong(split[2]));
			flowSortBeanWriable.setSumFlow(Long.parseLong(split[3]));
			text.set(phoneNo);
			context.write(flowSortBeanWriable,text);
		}
	}

	public static class FlowCountSortReducer extends Reducer<FlowSortBeanWriable, Text, Text, FlowSortBeanWriable>{
		@Override
		protected void reduce(FlowSortBeanWriable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text text = values.iterator().next();
			context.write(text,key);
		}
	}
}
