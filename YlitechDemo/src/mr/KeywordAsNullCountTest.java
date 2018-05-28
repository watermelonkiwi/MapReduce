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
 * 部分关键词为空，查询非空记录条数
 * input:hdfs://192.168.1.129/ylitech_demo/mr/replaceString_output/
 * output:hdfs://192.168.1.129/ylitech_demo/mr/keywordAsNullCount_output/
 * @author yz
 */

public class KeywordAsNullCountTest {
	
	static class KeywordAsNullCountTestMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text("key");
		private Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = new String(value.getBytes(), 0, value.getLength(), "GB18030");	// 读取格式为GB18030的文本转化成UTF-8
			
			if (line.trim().length() > 0) {
				v.set(line);
				context.write(k, v);
			}
		}
	}
	
	static class KeywordAsNullCountTestReducer extends Reducer<Text, Text, Text, NullWritable> {
		private Text k = new Text();
		private NullWritable v = NullWritable.get();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			
			for (Text line : values) {
				String[] fields = line.toString().split("\\\\");
				
				if (fields[2].length() > 0) {
					sum++;
				}
			}
			
			k.set(Long.toString(sum));
			context.write(k, v);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(KeywordAsNullCountTest.class);
		
		job.setMapperClass(KeywordAsNullCountTestMapper.class);
		job.setReducerClass(KeywordAsNullCountTestReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		//如果没有这个的话,中文会乱码
		job.setOutputFormatClass(GbkOutputFormat.class);

		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.129/ylitech_demo/mr/replaceString_output/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.129/ylitech_demo/mr/keywordAsNullCount_output/"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
