package mission2;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;

import mission1.Mission1;
import mission2.mapper.TheMapper;
import mission2.reducer.TheReducer;
import mission2.tools.TheKey;
import tools.BaseMission;

/**
 * 第二个任务：
 * <pre>
 * 对人名统计同现次数。
 * 输入：
 * 狄云 戚芳 戚芳 戚长发 卜垣
 * 戚芳 卜垣 卜垣
 * 输出：
 * &lt;狄云，戚芳&gt; 1       &lt;戚长发，狄云&gt; 1
 * &lt;狄云，戚长发&gt; 1     &lt;戚长发，戚芳&gt; 1
 * &lt;狄云，卜垣&gt; 1       &lt;戚长发，卜垣&gt; 1
 * &lt;戚芳，狄云&gt; 1       &lt;卜垣，狄云&gt; 1
 * &lt;戚芳，戚长发&gt; 1     &lt;卜垣，戚芳&gt; 2
 * &lt;戚芳，卜垣&gt; 2       &lt;卜垣，戚长发&gt; 1
 * </pre>
 * @author RailgunHasmter（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/28
 */

public class Mission2 extends BaseMission {
  /**
   * 任务二输出文件夹的名字，会被后面要依赖的任务调用。
   */
  public static final String output = "mission2";

  public Mission2(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf() {
    conf.set("input", args[1] + "/" + Mission1.output);
    conf.set("output", args[1] + "/" + output);
  }

  @Override
  protected void setupJob() {
    job.setMapperClass(TheMapper.class);
    job.setReducerClass(TheReducer.class);

    job.setMapOutputKeyClass(TheKey.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(TheKey.class);
    job.setOutputValueClass(IntWritable.class);
  }

  @Override
  protected String getDependecies() {
    return "1";
  }

  @Override
  public boolean isWorking() {
    return true;
  }
}