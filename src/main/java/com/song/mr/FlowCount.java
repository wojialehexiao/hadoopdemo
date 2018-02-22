package com.song.mr;

import com.song.bean.FlowBeanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Duo Nuo on 2018/2/3 0003.
 */
public class FlowCount {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();

		Job job = Job.getInstance(configuration);

		job.setJarByClass(WordCount.class);

		job.setMapperClass(FlowMapper.class);
		job.setMapOutputKeyClass(Text.class);


		job.setReducerClass(FlowReducer.class);
		job.setNumReduceTasks(5);
		job.setPartitionerClass(FlowPartition.class);

		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBeanWritable.class);

		FileInputFormat.setInputPaths(job,args[0]);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}

	public static class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBeanWritable>{

		FlowBeanWritable beanWritable = new FlowBeanWritable();
		Long counter = 0L;
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String line = value.toString();
				String[] split = line.split("\t");
				String flowKey = split[1];
				beanWritable.setUpFlow(Long.parseLong(split[8]));
				beanWritable.setdFlow(Long.parseLong(split[9]));
				context.write(new Text(flowKey), beanWritable);
			}catch (Exception e){
				counter ++;
				System.out.println(counter);
				e.printStackTrace();
			}
		}
	}

	private static class FlowReducer extends Reducer<Text, FlowBeanWritable, Text, FlowBeanWritable>{
		FlowBeanWritable beanWritable = new FlowBeanWritable();
		@Override
		protected void reduce(Text key, Iterable<FlowBeanWritable> values, Context context) throws IOException, InterruptedException {

			Long totalUpFlow = 0L;
			Long totalDFlow = 0L;

			for(FlowBeanWritable f : values){
				totalUpFlow += f.getUpFlow();
				totalDFlow += f.getdFlow();
			}

			beanWritable.setUpFlow(totalUpFlow);
			beanWritable.setdFlow(totalDFlow);
			context.write(key,beanWritable);
		}
	}
}

class FlowPartition extends Partitioner<Text, FlowBeanWritable>{

	Map<String,Integer> map = new HashMap<String, Integer>();

	FlowPartition(){
		map.put("135",1);
		map.put("136",2);
		map.put("137",3);
		map.put("138",4);

	}

	public int getPartition(Text text, FlowBeanWritable flowBeanWritable, int i) {
		String s = text.toString();
		String substring = s.substring(0, 3);
		Integer num = map.get(substring);

		return num == null ? 4 : num;
	}
}
