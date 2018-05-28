package mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 关键词出现次数排序，取前10
 * @author yz
 *
 */
public class KeywordSortAsCountTest {

	static class KeywordSortAsCountTestMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text k = new Text();
		private IntWritable v = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			//String line = new String(value.getBytes(), 0, value.getLength(), "GB18030");	// 读取格式为GB18030的文本转化成UTF-8
			String line = value.toString();
			
			if (line.trim().length() > 0) {
				String[] fields = line.split("\\\\");
				String word = fields[2];
				k.set(word);
				context.write(k, v);
			}
		}
	}
	
	static class KeywordSortAsCountTestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		Map<String, Integer> map = null;
		private Text k = new Text();
		private IntWritable v = new IntWritable();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			map = new HashMap<String, Integer>();
		}
		
		// <新闻, [1, 1, 1, 1, 1, 1, 1]>    <娱乐, [1, 1, 1, 1]> ...
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			
			map.put(key.toString(), sum);
			//context.write(k, v);
		}
		
		//<新闻, 7>    <娱乐, 4>  ...
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {			
			List<Map.Entry<String, Integer>> infoIds = new ArrayList<Map.Entry<String, Integer>>(map.entrySet());
			//对Hashmap根据value进行排序
			Collections.sort(infoIds, new Comparator<Map.Entry<String, Integer>>() {
				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o2.getValue() - o1.getValue();
				}
			});
			
			int count = 0;
			for (int i = 0; i < infoIds.size(); ++i) {
				String key = infoIds.get(i).getKey();
				int value = infoIds.get(i).getValue();
				
				k.set(key);
				v.set(value);
				context.write(k, v);
				count++;
				if (count >= 10) 
					break;
			}
		}
		
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(KeywordSortAsCountTest.class);

		job.setMapperClass(KeywordSortAsCountTestMapper.class);
		job.setReducerClass(KeywordSortAsCountTestReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//如果没有这个的话,中文会乱码
		//job.setOutputFormatClass(GbkOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.1.129/ylitech_demo/mr/formatUTF8_output/"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.1.129/ylitech_demo/mr/keywordSortAsCount_output/"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
