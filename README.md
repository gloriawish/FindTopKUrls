# 描述
100GB URL文件，使用1GB内存计算出出现次数TOP 100的URL和出现次数

# 思路

100GB数据无法一次加载进入内存中，可采用分治法，顺序读文件，使用URL的hash值后根据余数划分到不同的文件中；如果其中的文件超过了限制大小的继续划分直到所有的文件都小于限定大小；对于每个小文件，使用HashMap统计URL的出现频率，然后使用统计结果建立小顶堆；最后将所有的小顶堆合并后得到一个最终小顶堆；

# 测试

执行`maven install`

执行`java -jar topk-1.0.0-jar-with-dependencies.jar -g p "./data/" -s 100 ` 生成100G测试数据

执行`java -jar topk-1.0.0-jar-with-dependencies.jar -f "./data/input.txt" -k 100` 获取Top100数据