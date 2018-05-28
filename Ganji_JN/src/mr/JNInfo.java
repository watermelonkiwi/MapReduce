package mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
1	历城	120	2999
2	天桥	75	1500
3	市中	60	1800
4	历城	27	1200
5	天桥	50	1200
6	历城	108	1980
7	市中	60	2200
8	历城	70	2000
9	槐荫	120	6800
10	市中	60	1600
11	槐荫	86	1800
12	槐荫	121	2300
 * @author: yangzheng
 * @date: 2017年10月26日
 */

public class JNInfo {
	
	static class JNInfoMapper extends Mapper<LongWritable, Text, Text, DataBean> {
		private Text k = new Text();
		private DataBean v = new DataBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			
			String location;	//区名
			long area;	//面积
			float price;	//价格
			float avprice;	//均价
			long count;	//区域的出租屋总量
			
			if (fields.length == 4) {	//1	历城	120	2999
				location = fields[1];
				area = Long.parseLong(fields[2]);
				if("面议".equals(fields[3])) {
					price = 0;
				} else {
					price = Float.parseFloat(fields[3]);
				}
				avprice = price/(float)(area);
				count = 0;
				
			} else {	// 1		120	2999
				location = "其它";
				area = Long.parseLong(fields[1]);
				if("面议".equals(fields[2])) {
					price = 0;
				} else {
					price = Float.parseFloat(fields[2]);
				}
				avprice = price/(float)(area);
				count = 0;
			}
			k.set(location);
			v.set(location, area, price, avprice, count);
			context.write(k, v);
		}
	}
	
	
	static class JNInfoReducer extends Reducer<Text, DataBean, Text, DataBean> {
		private DataBean v = new DataBean();
		
		//area + "\t" + price + "\t" + avprice + "\t" + count;
		//历城	area	price 0 0
		@Override
		protected void reduce(Text key, Iterable<DataBean> values, Context context) throws IOException, InterruptedException {
			long sum_area = 0;	//面积
			float sum_price = 0;	//价格
			float avprice;	//均价
			long count = 0;	//区域的出租屋总量
			
			for (DataBean value : values) {
				if (value.getAvprice() > 10 && value.getAvprice() < 100) {
					sum_area += value.getArea();
					sum_price += value.getPrice();
				}
				count++;
			}
			avprice = sum_price / (float)sum_area;
		
			v.set(key.toString(), sum_area, sum_price, avprice, count);
			context.write(key, v);
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JNInfo.class);
		
		job.setMapperClass(JNInfoMapper.class);
		job.setReducerClass(JNInfoReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DataBean.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);

		FileInputFormat.addInputPath(job, new Path("hdfs://desktop0/ganji_jn/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://desktop0/ganji_jn/output"));
		
		boolean res = job.waitForCompletion(true);
		System.exit(res ? 0 : 1);
	}
}
