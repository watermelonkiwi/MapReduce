# YlitechDemo（以利天诚中MR所有案例）
[MapReduce编程](http://124.232.152.147:8081/ylitech-bd/portal/coursePlay/2102078c9ad44ad39ca5b8414a93d214)

## GB18030格式转换成UTF8实践 （FormatUTF8Test.java）
使用Mapreduce方式将GB18030格式的转换成UTF8格式的文件     

编写MR程序，将GB18030格式的文件转换成UTF-8格式的文件。在map方法里添加代码：  
```
  String line = new String(value.getBytes(), 0, value.getLength(),"GB18030");
  //读取格式为GB18030的文本转化成UTF-8，由于FileInputFormat默认的编码格式是UTF-8，采用该行代码将gb18030格式的转换成utf-8
```
## 字符替换实践 （ReplaceStringTest.java）
使用Mapreduce方式将\字符替换成：字符  

编写MR程序，将访问总量与关键词对应url之间的\号改为:号
首先将字符串以\字符切割，然后在将切割后的字符连接起来，将访问总量与URL之间的用：连接即可。
代码如下：  
```
String[] strVal = line.toString().split("\\\\");
String result = strVal[0] + "\\" + strVal[1] + "\\" + strVal[2]+ "\\" + strVal[3] + "\\" + strVal[4] + ":" + strVal[5];
```

## 部分关键词为空，查询非空记录条数实践 （KeywordAsNullCountTest.java）
使用Mapreduce方式实现部分关键词为空，查询非空记录条数  

编写MR程序，实现部分关键词为空，查询非空记录条数，将字符串读取过来，然后以字符\切分，在判断关键词不为空的记录数，最后输出非空记录的条数，代码如下：  
```
  int sum = 0;
for (Text line : values) {
// 20170102\id1\新闻\1211\120\http://www.baidu.com/news
String[] strVal = line.toString().split("\\\\");
  System.out.println(strVal[2]);
  if(strVal[2].length()>0)
sum++;			
}
```
## 统计不同的ID数量实践 （DiffIDSumTest.java）
使用Mapreduce方式实现统计不同的ID数量           

编写MR程序，实现统计不同的ID数量，对字符串进行切割，取出ID，然后放入ArrayList中  
如果ArrayList包含该ID，就不保存，最后统计出不同的ID。  
代码如下：  
```
ArrayList<String> list = new ArrayList<String>();
for (Text line : values) {
	String[] strVal = line.toString().split("\\\\");
	if (!list.contains(strVal[1]))
		list.add(strVal[1]);
}
```

## 关键词出现次数排序，取前10实践 （KeywordSortAsCountTest.java）
使用Mapreduce方式实现关键词出现次数排序，取前10   

编写MR程序，实现关键词出现次数排序，取前10，在map方法中，将字符串进行切割，然后取出关键词，并标记为1;在reduce方法中，统计每个不同关键词的数量，并保存到hashMap中，最后进行排序，取出top10.  
代码如下：  
```
List<Map.Entry<String, Integer>> infoIds = new ArrayList<Map.Entry<String, Integer>>(
		map.entrySet());
Collections.sort(infoIds,
		new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1,
					Map.Entry<String, Integer> o2) {
				return o2.getValue()-o1.getValue();
			}
		});
int count=0;
for (int i = 0; i <infoIds.size() ; i++) {
	String mKey = infoIds.get(i).getKey().toString();
	String mValue = infoIds.get(i).getValue().toString();

	System.out.println(mKey + "," + mValue);
	context.write(new Text(mKey), new Text(mValue));
	count++;
	if(count>=10) break;
}
```

## HashMap的两种排序方式 （Test.java）
```
List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(map.entrySet());
		
Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
	@Override
	public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
		return o2.getValue() - o1.getValue();	//根据value排序
		//return (o1.getKey()).toString().compareTo(o2.getKey());	//根据key排序
	}
});
```