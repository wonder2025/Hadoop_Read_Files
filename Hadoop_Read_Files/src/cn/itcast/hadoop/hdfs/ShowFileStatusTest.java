package cn.itcast.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

//查询文件系统

public class ShowFileStatusTest {
    private MiniDFSCluster cluster;
//    private DistributedFileSystem fs;
    private FileSystem fs;
    public String file="hdfs://172.31.238.103:8020/user/scdx03/secondarysort_out/part-r-00000";
    public String dir="hdfs://172.31.238.103:8020/user/scdx03/secondarysort_out";
    @Before
    public void setUp() throws IOException{
//        Configuration conf = new Configuration();
//        if(System.getProperty("test.build.data")==null){
//            System.setProperty("test.build.data","/tmp");
//        }
//        cluster=new MiniDFSCluster.Builder(conf).build();
//        fs=cluster.getFileSystem();


        Configuration conf = new Configuration();
        fs = FileSystem.get(URI.create(file), conf);
        OutputStream out=fs.create(new Path(file));
        out.write("content".getBytes("UTF-8"));
        out.close();
    }
    @After
    public void tearDown() throws IOException{
        if(fs!=null){
            fs.close();
        }
        if(cluster!=null){
            cluster.shutdown();
        }
    }
    @Test(expected= FileNotFoundException.class)
    public void throwsFileNotFoundForNonExistentFile() throws IOException{
        fs.getFileStatus(new Path("no-such-file"));
    }
    @Test
    public void fileStatusForFile() throws IOException{
        Path pfile=new Path(file);
        FileStatus stat=fs.getFileStatus(pfile);
        assertThat(stat.getPath().toUri().getPath(),is("/user/scdx03/secondarysort_out/part-r-00000"));
        assertThat(stat.isDirectory(),is(false));
        assertThat(stat.getLen(),is(7L));
//        assertThat(stat.getModificationTime(),is(lessThanOrEqualTo(System.currentTimeMillis())));
        //文件副本数目
        assertThat(stat.getReplication(),is((short)3));
        assertThat(stat.getBlockSize(),is(128*1024*1024L));
        //文件所有者/本机名称
        assertThat(stat.getOwner(),is(System.getProperty("user.name")));
        assertThat(stat.getGroup(),is("supergroup"));
        assertThat(stat.getPermission().toString(),is("rw-r--r--"));
    }
    @Test
    public void fileStatusForDirectory() throws IOException{
        Path pdir=new Path(dir);
        FileStatus stat=fs.getFileStatus(pdir);
        assertThat(stat.getPath().toUri().getPath(),is("/user/scdx03/secondarysort_out"));
        assertThat(stat.isDirectory(),is(true));
        assertThat(stat.getLen(),is(0L));
//        assertThat(stat.getModificationTime(),is(lessThanOrEqualTo(System.currentTimeMillis())));
        assertThat(stat.getReplication(),is((short)0));
        assertThat(stat.getBlockSize(),is(0L));
        assertThat(stat.getOwner(),is(System.getProperty("user.name")));
        assertThat(stat.getGroup(),is("supergroup"));
        assertThat(stat.getPermission().toString(),is("rw-r--r--"));
    }
}

