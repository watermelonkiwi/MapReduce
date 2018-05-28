# MapReduce编程与算法设计

对日志数据中的上下行流量信息汇总，并输出按照总流量倒序排序的结果

## [provinceflow](https://github.com/yangzheng0515/MapReduce/tree/master/provinceflow) （Mapreduce中的分区Partitioner）
根据归属地输出流量统计数据结果到不同文件，以便于在查询统计结果时可以定位到省级范围进行

## [weblogwash](https://github.com/yangzheng0515/MapReduce/tree/master/weblogwash) （web日志预处理）
对web访问日志中的各字段识别切分，去除日志中不合法的记录，根据KPI统计需求，生成各类访问请求过滤数据

## [mapsidejoin](https://github.com/yangzheng0515/MapReduce/tree/master/mapsidejoin) （map端join算法实现、缓存文件到所有的maptask）
通过将关联的条件作为map输出的key，将两表满足join条件的数据并携带数据所来源的文件信息，发往同一个reduce task，在reduce中进行数据的串联

## [sharedfriends](https://github.com/yangzheng0515/MapReduce/tree/master/sharedfriends) （社交粉丝数据分析）
求出哪些人两两之间有共同好友，及他俩的共同好友都有谁？

## [Ganji_JN](https://github.com/yangzheng0515/MapReduce/tree/master/Ganji_JN)（济南租房数据处理）
爬取赶集网上济南所有租房信息，并统计结果

## [YlitechDemo](https://github.com/yangzheng0515/MapReduce/tree/master/YlitechDemo) （以利天诚中MR所有案例）
[MapReduce编程](http://124.232.152.147:8081/ylitech-bd/portal/coursePlay/2102078c9ad44ad39ca5b8414a93d214)