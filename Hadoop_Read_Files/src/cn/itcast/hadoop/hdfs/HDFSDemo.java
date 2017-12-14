package cn.itcast.hadoop.hdfs;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import org.junit.Test;

public class HDFSDemo {
	private FileSystem fs = null;
	@Test
	public   void  hdfsDemo()throws IOException, URISyntaxException {

		String uri = "hdfs://172.31.238.103:8020/user/scdx03/secondarysort_out/part-r-00000";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		InputStream in = null;
		try {
			in = fs.open(new Path(uri));
//			IOUtils.copyBytes(in, System.out, 4096, false);
			FileOutputStream out = new FileOutputStream(new File("D:/part/Demo.txt"));
//			输出到文件
//			IOUtils.copyBytes(in, out, 2048, true);
//			输出到控制台
			IOUtils.copyBytes(in, System.out, 2048, true);
		} finally {
			IOUtils.closeStream(in);
		}
	}
	@Test
	public  void  fileSystemDoubleCat() throws IOException, URISyntaxException {
		String uri = "hdfs://172.31.238.103:8020/user/scdx03/secondarysort_out/part-r-00000";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		FSDataInputStream in = null;
		try {
			in = fs.open(new Path(uri));
//			输出到控制台
			IOUtils.copyBytes(in, System.out, 2048, false);
//			定位到文件开头
			in.seek(3);
			System.out.println("-------------------line-----------------");
//			输出到控制台
			IOUtils.copyBytes(in, System.out, 2048, false);
		} finally {
			IOUtils.closeStream(in);
		}
	}
	@Test
//	将本地文件复制到Hadoop文件系统
	public  void  fileCopyWithProgress() throws IOException, URISyntaxException {
		String localSrc="D://hdfsTest.txt";
		String dst="hdfs://172.31.238.103:8020/user/scdx03/secondarysort_out/part-r-00000";;
		InputStream in=new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		OutputStream out=fs.create(new Path(dst), new Progressable() {
			@Override
			public void progress() {
				System.out.println(".");
			}
		});
//		追加
//		OutputStream out=fs.append(new Path(dst));
		IOUtils.copyBytes(in, out, 2048, true);
	}
	@Test
	//显示hdfs的目录下的所有文件
	public void listStatus() throws IOException {
		String[] uri = {"hdfs://172.31.238.103:8020/user/scdx03/secondarysort_out"};
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri[0]), conf);
		Path[] paths=new Path[uri.length];
		for (int i=0;i<paths.length;i++){
			paths[i]=new Path(uri[i]);
		}
		FileStatus[] stauts=fs.listStatus(paths);
		Path[] listedPaths=FileUtil.stat2Paths(stauts);
		for (Path p:listedPaths){
			System.out.println(p);
		}
	}

	@Test
	//删除文件
	public void delete() throws IOException {
		String uri = "hdfs://172.31.238.103:8020/user/scdx03/output";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		fs.delete(new Path(uri),true);

	}

}
