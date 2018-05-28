package mr;

import java.io.IOException;

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

/**
 * 将访问总量与关键词对应url之间的\号改为:号
 * @author yz
 * input:hdfs://192.168.1.129/ylitech_demo/mr/formatUTF8_output/
 * output:hdfs://192.168.1.129/ylitech_demo/mr/replaceString_output/

test:
String line = "20170102\\id1\\新闻\\\1211\\120\\http://www.baidu.com/news";
String[] fields = line.split("\\\\");
//20170102\id1\新闻\1211\120\http://www.baidu.com/news
System.out.println(fields[0] + "\\" + fields[1] + "\\" + fields[2] + "\\" + fields[3] + "\\" + fields[4] + ":" + fields[5]);
//20170102\id1\新闻\1211\120:http://www.baidu.com/news
 */

public class ReplaceStringTest {
	
	static class ReplaceStringTestMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text("key");
		private Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = new String(value.getBytes(), 0, value.getLength(), "GB18030");
			
			String[] fields = line.split("\\\\");
			//20170102\id1\新闻\1211\120\http://www.baidu.com/news
			//System.out.println(fields[0] + "\\" + fields[1] + "\\" + fields[2] + "\\" + fields[3] + "\\" + fields[4] + ":" + fields[5]);
			//20170102\id1\新闻\1211\120:http://www.baidu.com/news
			line = fields[0] + "\\" + fields[1] + "\\" + fields[2] + "\\" + fields[3] + "\\" + fields[4] + ":" + fields[5];
			
			if (line.trim().length() > 0) {
				v.set(line);
				
				context.write(k, v);
			}
		}
	}
	
	
	static class ReplaceStringTestReducer extends Reducer<Text, Text, Text, NullWritable> {
		private Text k = new Text();
		private NullWritable v = NullWritable.get();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text line : values) {
				k.set(line);
				
				context.write(k, v);
			}
			
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ReplaceStringTest.class);
		
		job.setMapperClass(ReplaceStringTestMapper.class);
		job.setReducerClass(ReplaceStringTestReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//如果没有这个的话,中文会乱码
		job.setOutputFormatClass(GbkOutputFormat.class);

		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.129/ylitech_demo/mr/formatUTF8_output/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.129/ylitech_demo/mr/replaceString_output/"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
