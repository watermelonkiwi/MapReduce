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
1	����	120	2999
2	����	75	1500
3	����	60	1800
4	����	27	1200
5	����	50	1200
6	����	108	1980
7	����	60	2200
8	����	70	2000
9	����	120	6800
10	����	60	1600
11	����	86	1800
12	����	121	2300
 * @author: lzq
 * @date: 2017��10��26��
 */

public class JNInfo {
	
	static class JNInfoMapper extends Mapper<LongWritable, Text, Text, DataBean> {
		private Text k = new Text();
		private DataBean v = new DataBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			
			String location;	//����
			long area;	//���
			float price;	//�۸�
			float avprice;	//����
			long count;	//����ĳ���������
			
			if (fields.length == 4) {	//1	����	120	2999
				location = fields[1];
				area = Long.parseLong(fields[2]);
				if("����".equals(fields[3])) {
					price = 0;
				} else {
					price = Float.parseFloat(fields[3]);
				}
				avprice = price/(float)(area);
				count = 0;
				
			} else {	// 1		120	2999
				location = "����";
				area = Long.parseLong(fields[1]);
				if("����".equals(fields[2])) {
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
		//����	area	price 0 0
		@Override
		protected void reduce(Text key, Iterable<DataBean> values, Context context) throws IOException, InterruptedException {
			long sum_area = 0;	//���
			float sum_price = 0;	//�۸�
			float avprice;	//����
			long count = 0;	//����ĳ���������
			
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
