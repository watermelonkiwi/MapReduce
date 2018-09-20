package mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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

/**
 * 统计不同的ID数量有多少
 * input:hdfs://192.168.1.129/ylitech_demo/mr/replaceString_output/
 * output:hdfs://192.168.1.129/ylitech_demo/mr/diffIDSum_output/
 * @author lzq
 *
 */
public class DiffIDSumTest {

	static class DiffIDSumTestMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text("key");
		private Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String line = new String(value.getBytes(), 0, value.getLength(), "GB18030");	// 读取格式为GB18030的文本转化成UTF-8
			
			if (line.trim().length() > 0) {
				v.set(line);
				context.write(k, v);
			}
		}
	}
	
	
	static class DiffIDSumTestReducer extends Reducer<Text, Text, IntWritable, NullWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			 int sum = 0;
			 List<String> ids = new ArrayList<String>();
			 
			 for (Text value : values) {
				 String id_str = value.toString().split("\\\\")[1];
				 if (!ids.contains(id_str)) {
					 ids.add(id_str);
				 }
			 }
			 sum = ids.size();
			 context.write(new IntWritable(sum), NullWritable.get());
		}
	}
	 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(DiffIDSumTest.class);

		job.setMapperClass(DiffIDSumTestMapper.class);
		job.setReducerClass(DiffIDSumTestReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		//如果没有这个的话,中文会乱码
		//job.setOutputFormatClass(GbkOutputFormat.class);	//没注释时会没有数字输出,不知道为什么. 注释了,输出结果为3
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.129/ylitech_demo/mr/replaceString_output/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.129/ylitech_demo/mr/diffIDSum_output/"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
