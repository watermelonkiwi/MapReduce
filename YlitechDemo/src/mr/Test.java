package mr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * HashMap的两种排序方式
 * @author yz
 *
 */
public class Test {
	public static void main(String[] args) {
		
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("新闻", 7);
		map.put("其他", 2);
		map.put("娱乐", 4);
		
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(map.entrySet());
		
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o2.getValue() - o1.getValue();	//根据value排序
				//return (o1.getKey()).toString().compareTo(o2.getKey());	//根据key排序
			}
		});
		
		for (int i = 0; i < list.size(); ++i) {
			System.out.println(list.get(i).getKey() + ":" + list.get(i).getValue().toString());
		}
	}
}
