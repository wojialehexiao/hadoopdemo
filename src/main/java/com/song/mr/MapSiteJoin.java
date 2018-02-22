package com.song.mr;

import com.song.bean.FileConstant;
import com.song.bean.OrderBeanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Duo Nuo on 2018/2/8 0008.
 */
public class MapSiteJoin {


	static class MapSiteJoinMapper extends Mapper<LongWritable, Text, OrderBeanWritable, NullWritable>{
		FSDataInputStream inputStream = null;
		Map<String,String> productMap = new HashMap<String,String>();

		OrderBeanWritable orderBeanWritable = new OrderBeanWritable();

		NullWritable nullWritable = NullWritable.get();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			URI[] cacheFiles = context.getCacheFiles();
			URI uri = null;
			for(URI u : cacheFiles){
				String path = u.toString();
				if(FileConstant.PRODUCT_PATH.equals(path)){
					uri = u;
				}
			}

			FileSystem fileSystem = FileSystem.get(uri, context.getConfiguration());
			inputStream = fileSystem.open(new Path(uri.getPath()));
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

			String line = null;

			while ((line = br.readLine()) != null){
				String[] split = line.split("\t");
				String pid = split[0];
				productMap.put(pid,line);
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] split = line.split("\t");
			Integer id = Integer.parseInt(split[0]);
			String date = split[1];
			String pid = split[2];
			Integer amount = Integer.parseInt(split[3]);

			String product = productMap.get(pid);
			String[] split1 = product.split("\t");
			String pname = split1[1];
			Integer categoryId = Integer.parseInt(split1[2]);
			Double price = Double.parseDouble(split1[3]);

			orderBeanWritable.set(id,date,pid,amount,pname,categoryId,price);

			context.write(orderBeanWritable,nullWritable);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(inputStream != null){
				inputStream.close();
			}
		}
	}


	public static void main(String[] args) throws Exception {

		System.setProperty("user.name","song");

		Configuration configuration = new Configuration();

		Job job = Job.getInstance(configuration);

		job.setJarByClass(MapSiteJoin.class);

		job.setMapperClass(MapSiteJoinMapper.class);

		job.setMapOutputKeyClass(OrderBeanWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(OrderBeanWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);


		job.setCacheFiles(new URI[]{new URI(FileConstant.PRODUCT_PATH)});

		FileInputFormat.setInputPaths(job,args[0]);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}
}
