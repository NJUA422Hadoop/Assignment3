# Assignment3

环境：
- Hadoop 2.7.1
- Hbase 1.2.6

语言：java7

内容：倒排索引实验

运行：

1. 自带依赖的包：hadoop jar XXX-jar-with-dependencies.jar InvertedIndexAssignment InputPath OutputPath

MacOS由于大小写不敏感问题：LICENSE和license冲突，所以需要先运行：

- zip -d XXX-jar-with-dependencies.jar META-INF/LICENSE

- jar tvf XXX-jar-with-dependencies.jar | grep -i license

2. java -cp XXX-jar-with-dependencies.jar Mission Wuxia OutputFilePath （单机处理文件，请使用java7（java8似乎也兼容，但java10不行））

[项目地址](https://github.com/NJUA422Hadoop/Assignment3)

[小组地址](https://github.com/NJUA422Hadoop)
