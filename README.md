# BigData-Lab
Lab for Lesson 'Big-Data'

每个project分别对应不同的Task，为了方便测试将功能拆分分别实现。以下为不同的Task的输入测试方法：
project——MainLab是主程序，调用其他的project
文件夹Task2直接放在桌面目录上，输入输出文件目录都在其中

Task1(WordConcurrence):
/home/santiago3/桌面/task2/task2/novels/
/home/santiago3/桌面/task2/output1/

Task2(MenPairs):
/home/santiago3/桌面/task2/output1/
/home/santiago3/桌面/task2/output2/

Task3(MenMap):
/home/santiago3/桌面/task2/output2/part-r-00000
/home/santiago3/桌面/task2/output3/

Task4(MenPagerank):
/home/santiago3/桌面/task2/output3/part-r-00000
/home/santiago3/桌面/task2/output4/
100

Task5(LPA):
/home/santiago3/桌面/task2/output3/part-r-00000
/home/santiago3/桌面/task2/output5/

Task6(LabelProcess):
/home/santiago3/桌面/task2/output5/FinalLabel/part-r-00000
/home/santiago3/桌面/task2/output6/

整合后的Task(FinalLab——输入简单化，同时完成了配置打包jar):
/home/santiago3/桌面/task2/task2/novels/
/home/santiago3/桌面/task2/

ps:原本的TotalLab整合后并不能在集群上跑通(jdk1.7)，会出现各种奇怪的问题，新的FinalLab则解决了这些问题！
