
# hui-bigdata-spark

# 项目介绍
1. spark相关的一些demo
2. springboot+spark的脚手架开发（开箱即用）

# 项目博客相关
```
【Spark】Springboot搭建spark脚手架 https://blog.csdn.net/HuHui_/article/details/89391509
【Spark】SparkSql分析结果写入Mysql https://blog.csdn.net/HuHui_/article/details/83964233
【Spark】SparkRDD开发手册（JavaAPI函数式编程）https://blog.csdn.net/HuHui_/article/details/83905308
【Spark】Spark运行报错Task not serializable https://blog.csdn.net/HuHui_/article/details/83905302
```
# 脚手架相关
## Note

- 项目结构：  

├─hui-bigdata-spark-common（公共组件）  
│  └─src  
│      └─main  
│          └─java  
│              └─com  
│                  └─hui  
│                      └─bigdata  
│                          └─common  
│                              ├─spark  
│                              └─utils  
├─hui-bigdata-spark-starter（启动类）  
│  └─src  
│      └─main  
│          ├─java  
│          │  └─com  
│          │      └─hui  
│          │          └─bigdata  
│          │              └─config  
│          └─resources  
├─hui-bigdata-statistics-a（统计模块A）  
│  └─src  
│      └─main  
│          └─java  
│              └─com  
│                  └─hui  
│                      └─bigdata  
│                          └─statistics  
│                              ├─config  
│                              ├─job  
│                              └─model  
├─hui-bigdata-statistics-b（统计模块B）  

## SparkApplication(程序启动入口)

原理是参数传入类路径通过反射获取类信息，并且使用到了springboot的implements CommandLineRunner让容器启动完成的时候执行。

- 划分模块的说明：
  1. 希望统计模块可复用。（如果公司有相同业务，只是数据源不一样，直接引入该模块做少量配置即可）
  2. starter 模块是程序的启动入口。不管什么spark统计任务，只需要把该脚手架复制一份，并且引入统计模块A/B/....即可
  3. demo是原生跑spark的一些例子
# Author
```
 作者：HuHui
 转载：欢迎一起讨论web和大数据问题,转载请注明作者和原文链接，感谢
```
