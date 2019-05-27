# 描述
100GB URL文件，使用1GB内存计算出出现次数TOP 100的URL和出现次数

# 思路

100GB数据无法一次加载进入内存中，可采用分治法，顺序读文件，使用URL的hash值后根据余数划分到不同的文件中；如果其中的文件超过了限制大小的继续划分直到所有的文件都小于限定大小；对于每个小文件，使用HashMap统计URL的出现频率，然后使用统计结果建立**小顶堆**；最后将所有的**小顶堆合并**后得到一个最终小顶堆；

# 优化思路

分割成为小文件后，每个文件的小顶堆使用线程池的方式进行计算，线程数量不宜过多一般和CPU核心数差不多



# 运行

执行`mvn install`编译生成可执行jar，存放在target目录下

进入target目录执行`java -jar topk-1.0.0-jar-with-dependencies.jar -g p "./data/" -s 100 ` 生成100G测试数据

然后执行`java -jar -Xms512m -Xmx1024m topk-1.0.0-jar-with-dependencies.jar -f "./data/input.txt" -k 100` 获取Top100数据

输出结果在同级目录的`result.txt`中