package test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyDBOutputFormat {

	// 定义输出路径  
    private static final String INPUT_PATH = "d:/user/input";  
  
    public static void main(String[] args) {  
  
        try {  
            // 创建配置信息  
            Configuration conf = new Configuration();  
  
            /* 
             * //对Map端的输出进行压缩 
             *  conf.setBoolean("mapred.compress.map.output", true);  
             * //设置map端输出使用的压缩类 
             *  conf.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class);  
             * //对reduce端输出进行压缩  
             * conf.setBoolean("mapred.output.compress", true);  
             * //设置reduce端输出使用的压缩类 
             * conf.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class); 
             */  
  
            // 添加配置文件(我们可以在编程的时候动态配置信息，而不需要手动去改变集群)  
            /* 
             * conf.addResource("classpath://hadoop/core-site.xml"); 
             * conf.addResource("classpath://hadoop/hdfs-site.xml"); 
             * conf.addResource("classpath://hadoop/hdfs-site.xml"); 
             */  
  
            // 通过conf创建数据库配置信息  
            DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test2", "root", "root");  
              
            /*// 创建文件系统 
            FileSystem fileSystem = FileSystem.get(new URI(OUT_PATH), conf); 
 
            // 如果输出目录存在就删除 
            if (fileSystem.exists(new Path(OUT_PATH))) { 
                fileSystem.delete(new Path(OUT_PATH), true); 
            }*/  
  
            // 创建任务  
            Job job = new Job(conf, MyDBOutputFormat.class.getName());  
  
            // 1.1 设置输入数据格式化的类和设置数据来源  
            job.setInputFormatClass(TextInputFormat.class);  
            FileInputFormat.setInputPaths(job, INPUT_PATH);  
            //1.2 设置自定义的Mapper类和Mapper输出的key和value的类型  
            job.setMapperClass(MyDBOutputFormatMapper.class);  
            job.setMapOutputKeyClass(LongWritable.class);  
            job.setMapOutputValueClass(User.class);  
  
            // 1.3 设置分区和reduce数量(reduce的数量和分区的数量对应，因为分区只有一个，所以reduce的个数也设置为一个)  
            job.setPartitionerClass(HashPartitioner.class);  
            job.setNumReduceTasks(1);  
  
            // 1.4 排序、分组  
            // 1.5 归约  
            // 2.1 Shuffle把数据从Map端拷贝到Reduce端  
  
            // 2.2 指定Reducer类和输出key和value的类型  
            job.setReducerClass(MyDBOutputFormatReducer.class);  
  
            // 2.3 设置输出的格式化类和设置将reduce端输出的key值对应user表  
            job.setOutputFormatClass(DBOutputFormat.class);  
            DBOutputFormat.setOutput(job, "user2", new String[] { "id", "name" });  
  
            // 提交作业 然后关闭虚拟机正常退出  
            System.exit(job.waitForCompletion(true) ? 0 : 1);  
  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
  
    /** 
     *  自定义Mapper类 
     * @author 廖钟民 
     * time : 2015年1月15日下午3:37:31 
     * @version 
     */  
    public static class MyDBOutputFormatMapper extends Mapper<LongWritable, Text, LongWritable, User>{  
          
        //创建写出去的value类型  
        private User user = new User();  
          
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, User>.Context context) throws IOException,  
                InterruptedException {  
            //对value进行切分  
            String[] splits = value.toString().split(",");  
            //封装user对象  
            user.setId(Integer.parseInt(splits[0]));  
            user.setName(splits[1]);  
            //把user对象作为value写出去  
            context.write(key, user);  
        }  
    }  
      
  
    /** 
     * 关键是写出去的key要为User对象 
     * 写出去的value值无所谓，为NullWritable都可以 
     * @author 廖钟民 
     * time : 2015年1月15日下午3:44:24 
     * @version 
     */  
    public static class MyDBOutputFormatReducer extends Reducer<LongWritable, User, User, Text> {  
          
        protected void reduce(LongWritable key, Iterable<User> values, Reducer<LongWritable, User, User, Text>.Context context) throws IOException,  
                InterruptedException {  
            for (User user : values){  
                context.write(user, new Text(new Text(user.getName())));  
            }  
        }  
          
    }  
}
