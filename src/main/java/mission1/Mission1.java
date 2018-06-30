package mission1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import mission1.combiner.TheCombiner;
import mission1.input.TheInputFormat;
import mission1.mapper.TheMapper;
import mission1.output.TheOutputFormat;
import mission1.partitioner.ThePartitioner;
import mission1.reducer.TheReducer;
import mission1.tools.TheKey;
import tools.BaseMission;

/**
 * 第一个任务：
 * <pre>
 * 对金庸小说数据集，根据人名进行分词。保存分词结果的人名。
 * 输入：
 * 狄云和戚芳一走到万家大宅之前，瞧见那高墙朱门、挂灯结彩的气派，心中都是暗自嘀咕。戚芳紧紧拉住了父亲的衣袖。戚长发正待向门公询问，忽见卜垣从门里出来，心中一喜，叫道：“卜贤侄，我来啦。”
 * 输出：
 * 狄云 戚芳 戚芳 戚长发 卜垣
 * </pre>
 * @author RailgunHasmter（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/26
 */

public class Mission1 extends BaseMission {
  /**
   * 任务一输出文件夹的名字，会被后面要依赖的任务调用。
   */
  public static final String output = "mission1";

  public Mission1(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf(int index) {
    conf.set("input", args[0]);
    conf.set("output", args[1] + "/" + output);
  }

  @Override
  protected Job setupJob(Job job) {
    job.setMapperClass(TheMapper.class);
    job.setReducerClass(TheReducer.class);

    job.setInputFormatClass(TheInputFormat.class);
    job.setOutputFormatClass(TheOutputFormat.class);

    job.setCombinerClass(TheCombiner.class);
    job.setPartitionerClass(ThePartitioner.class);

    // TheKey class 要求其必须实现Comparable<TheKey>，而不能通过继承的方式。
    // 如果一定要继承，请参考Text的实现。例：class A extends B implements WritableComparable<B>
    job.setMapOutputKeyClass(TheKey.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(TheKey.class);
    job.setOutputValueClass(Text.class);

    return job;
  }

  @Override
  public boolean isWorking() {
    return true;
  }
}