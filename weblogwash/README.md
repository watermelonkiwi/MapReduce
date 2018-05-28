# weblogwash （web日志预处理）

## 需求
对web访问日志中的各字段识别切分，去除日志中不合法的记录，根据KPI统计需求，生成各类访问请求过滤数据

日志数据中的各数据字段：
```
private String remote_addr;// 记录客户端的ip地址
private String remote_user;// 记录客户端用户名称,忽略属性"-"
private String time_local;// 记录访问时间与时区
private String request;// 记录请求的url与http协议
private String status;// 记录请求状态；成功是200
private String body_bytes_sent;// 记录发送给客户端文件主体内容大小
private String http_referer;// 用来记录从那个页面链接访问过来的
private String http_user_agent;// 记录客户浏览器的相关信息
private boolean valid = true;// 判断数据是否合法
```

清洗前
```
194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] "GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"
183.49.46.228 - - [18/Sep/2013:06:49:23 +0000] "-" 400 0 "-" "-"
163.177.71.12 - - [18/Sep/2013:06:49:33 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"
163.177.71.12 - - [18/Sep/2013:06:49:36 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"
101.226.68.137 - - [18/Sep/2013:06:49:42 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"
101.226.68.137 - - [18/Sep/2013:06:49:45 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"
60.208.6.156 - - [18/Sep/2013:06:49:48 +0000] "GET /wp-content/uploads/2013/07/rcassandra.png HTTP/1.0" 200 185524 "http://cos.name/category/software/packages/" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36"
222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] "GET /images/my.jpg HTTP/1.1" 200 19939 "http://www.angularjs.cn/A00n" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36"
222.68.172.190 - - [18/Sep/2013:06:50:08 +0000] "-" 400 0 "-" "-"
183.195.232.138 - - [18/Sep/2013:06:50:16 +0000] "HEAD / HTTP/1.1" 200 20 "-" "DNSPod-Monitor/1.0"
```

清洗後
```
true194.237.142.21-2013-09-18 06:49:18/wp-content/uploads/2013/07/rstudio-git3.png3040"-""Mozilla/4.0 (compatible;)"
true163.177.71.12-2013-09-18 06:49:33/20020"-""DNSPod-Monitor/1.0"
true163.177.71.12-2013-09-18 06:49:36/20020"-""DNSPod-Monitor/1.0"
true101.226.68.137-2013-09-18 06:49:42/20020"-""DNSPod-Monitor/1.0"
true101.226.68.137-2013-09-18 06:49:45/20020"-""DNSPod-Monitor/1.0"
true60.208.6.156-2013-09-18 06:49:48/wp-content/uploads/2013/07/rcassandra.png200185524"http://cos.name/category/software/packages/""Mozilla/5.0 (Windows
true222.68.172.190-2013-09-18 06:49:57/images/my.jpg20019939"http://www.angularjs.cn/A00n""Mozilla/5.0 (Windows
true183.195.232.138-2013-09-18 06:50:16/20020"-""DNSPod-Monitor/1.0"
true183.195.232.138-2013-09-18 06:50:16/20020"-""DNSPod-Monitor/1.0"
true66.249.66.84-2013-09-18 06:50:28/page/6/20027777"-""Mozilla/5.0 (compatible;
true221.130.41.168-2013-09-18 06:50:37/feed/3040"-""Mozilla/5.0 (Windows
```

