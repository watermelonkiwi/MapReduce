# provinceflow （Mapreduce中的分区Partitioner）

## 需求
根据归属地输出流量统计数据结果到不同文件，以便于在查询统计结果时可以定位到省级范围进行

## 分析
Mapreduce中会将map输出的kv对，按照相同key分组，然后分发给不同的reducetask
默认的分发规则为：根据key的hashcode%reducetask数来分发
所以：如果要按照我们自己的需求进行分组，则需要改写数据分发（分组）组件Partitioner
自定义一个CustomPartitioner继承抽象类：Partitioner
然后在job对象中，设置自定义partitioner： job.setPartitionerClass(CustomPartitioner.class)

