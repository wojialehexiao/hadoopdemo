package com.song.main;

import com.song.util.HdfsUtil;

import java.io.IOException;
import java.util.List;

/**
 * Created by Duo Nuo on 2018/2/7 0007.
 */
public class Main {

	public static void main(String[] args) throws Exception {

		System.setProperty("HADOOP_USER_NAME","hdfs");

		List<String> list = HdfsUtil.listAll("/backup/datamining/hzdata");
		for(String s : list){
			System.out.println(s);
		}

//		boolean mkdir = HdfsUtil.mkdir("/backup/datamining/datapay");
//		System.out.println(mkdir);
	}
}
