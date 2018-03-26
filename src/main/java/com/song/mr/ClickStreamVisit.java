package com.song.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.song.bean.PageViewsBean;
import com.song.bean.VisitBean;
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

public class ClickStreamVisit {
	static class ClickStreamVisitMapper
			extends Mapper<LongWritable, Text, Text, PageViewsBean> {
		PageViewsBean pvBean = new PageViewsBean();
		Text k = new Text();

		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PageViewsBean>.Context context)
				throws IOException, InterruptedException {
			try {
				String line = value.toString();
				String[] fields = line.split("\001");
				int step = Integer.parseInt(fields[5]);
				this.pvBean.set(fields[0], fields[1], fields[3], fields[4], step, fields[9], fields[7], fields[8], fields[6], fields[10]);
				this.k.set(this.pvBean.getSession());
				context.write(this.k, this.pvBean);
			} catch (Exception e) {

			}
		}
	}

	static class ClickStreamVisitReducer
			extends Reducer<Text, PageViewsBean, NullWritable, VisitBean> {
		protected void reduce(Text session, Iterable<PageViewsBean> pvBeans, Reducer<Text, PageViewsBean, NullWritable, VisitBean>.Context context)
				throws IOException, InterruptedException {
			ArrayList<PageViewsBean> pvBeansList = new ArrayList();
			for (PageViewsBean pvBean : pvBeans) {
				PageViewsBean bean = new PageViewsBean();
				try {
					BeanUtils.copyProperties(bean, pvBean);
					pvBeansList.add(bean);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			Collections.sort(pvBeansList, new Comparator<PageViewsBean>() {
				public int compare(PageViewsBean o1, PageViewsBean o2) {
					return o1.getStep() > o2.getStep() ? 1 : -1;
				}

			});
			VisitBean visitBean = new VisitBean();

			visitBean.setInPage(((PageViewsBean) pvBeansList.get(0)).getRequest());
			visitBean.setInTime(((PageViewsBean) pvBeansList.get(0)).getTimestr());

			visitBean.setOutPage(((PageViewsBean) pvBeansList.get(pvBeansList.size() - 1)).getRequest());
			visitBean.setOutTime(((PageViewsBean) pvBeansList.get(pvBeansList.size() - 1)).getTimestr());

			visitBean.setPageVisits(pvBeansList.size());

			visitBean.setRemote_addr(((PageViewsBean) pvBeansList.get(0)).getRemote_addr());

			visitBean.setReferal(((PageViewsBean) pvBeansList.get(0)).getReferal());
			visitBean.setSession(session.toString());

			context.write(NullWritable.get(), visitBean);
		}
	}

	public static void main(String[] args)
			throws Exception {

		Configuration conf = new Configuration();
		System.setProperty("user.name","song");
		System.setProperty("HADOOP_USER_NAME", "hadoop");


		Job job = Job.getInstance(conf);

		job.setJarByClass(ClickStreamVisit.class);

		job.setMapperClass(ClickStreamVisitMapper.class);
		job.setReducerClass(ClickStreamVisitReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageViewsBean.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VisitBean.class);

		FileInputFormat.setInputPaths(job, new Path[]{new Path("E:/BaiduNetdiskDownload/资料文件/day14/生成日志数据的python程序/pageviews")});
		FileOutputFormat.setOutputPath(job, new Path("D:/data/out001"));

		job.waitForCompletion(true);
	}
}
