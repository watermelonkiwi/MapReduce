package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataBean implements Writable {
	private String location;	//区名
	private long area;	//面积
	private float price;	//价格
	private float avprice;	//均价
	private long count;	//区域的出租屋总量
	
	public DataBean() {}
	
	public DataBean(String location, long area, float price, float avprice, long count) {
		this.location = location;
		this.area = area;
		this.price = price;
		this.avprice = avprice;	// = (float)price / area;
		this.count = count;
	}
	
	public void set(String location, long area, float price, float avprice, long count) {
		this.location = location;
		this.area = area;
		this.price = price;
		this.avprice = avprice;
		this.count = count;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public long getArea() {
		return area;
	}

	public void setArea(long area) {
		this.area = area;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	public float getAvprice() {
		return avprice;
	}

	public void setAvprice(float avprice) {
		this.avprice = avprice;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	@Override
	public String toString() {
//		return "DataBean [location=" + location + ", area=" + area + ", price=" + price + ", avprice=" + avprice
//				+ ", count=" + count + "]";
		return area + "\t" + price + "\t" + avprice + "\t" + count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.location = in.readUTF();
		this.area = in.readLong();
		this.price = in.readFloat();
		this.avprice = in.readFloat();
		this.count = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.location);
		out.writeLong(this.area);
		out.writeFloat(this.price);
		out.writeFloat(this.avprice);
		out.writeLong(this.count);
	}
}
