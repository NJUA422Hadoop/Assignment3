package mission3;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import mission2.Mission2;
import mission3.mapper.TheMapper;
import mission3.reducer.TheReducer;
import mission3.tools.TheValue;
import tools.BaseMission;

/**
 * 第三个任务：
 * <pre>
 * 构建人物关系图并归一化
 * 输入：
 * &lt;狄云,戚芳&gt; 1     &lt;戚长发,狄云&gt; 1
 * &lt;狄云,戚长发&gt; 1   &lt;戚长发,戚芳&gt; 1
 * &lt;狄云,卜垣&gt; 1     &lt;戚长发,卜垣&gt; 1
 * &lt;戚芳,狄云&gt; 1    &lt;卜垣,狄云&gt; 1
 * &lt;戚芳,戚长发&gt; 1  &lt;卜垣,戚芳&gt; 2
 * &lt;戚芳,卜垣&gt; 2    &lt;卜垣,戚长发&gt; 1
 * 输出：
 * 狄云[戚芳:0.33333|戚长发:0.333333|卜垣:0.333333]
 * 戚芳[狄云:0.25|戚长发:0.25|卜垣:0.5]
 * 戚长发[狄云:0.33333|戚芳:0.333333|卜垣:0.333333]
 * 卜垣[狄云:0.25|戚芳:0.5|戚长发:0.25]
 * </pre>
 * 首先是将统计出的人物共现次数结果，转换成邻接表的形式表示：每一行表示一个邻接关系。
 * “戚芳[狄云:0.25|戚长发:0.25|卜垣:0.5] ”
 * 表示了顶点“戚芳”，有三个邻接点，分别是“狄云”、“戚长发”和“卜垣”，对应三条邻接边，每条邻接边上分别具有不同的权重。
 * 这个邻接边的权重是依靠某个人与其他人共现的“概率”得到的，
 * 以“戚芳”为例，她分别与三个人（“狄云”共现1次、“戚长发”，共现1次、“卜垣”共现2次）有共现关系，
 * 则戚芳与三个人共现的“概率”分别为1/(1+1+2) = 0.25，1/(1+1+2) = 0.25， 2/(1+1+2) = 0.5。
 * 这三个“概率”值对应与三条边的权重。通过这种归一化，我们确保了某个顶点的出边权重的和为1。
 * @author RailgunHasmter（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/28
 */

public class Mission3 extends BaseMission {
  /**
   * 任务三输出文件夹的名字，会被后面要依赖的任务调用。
   */
  public static final String output = "mission3";

  public Mission3(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf() {
    conf.set("input", args[1] + "/" + Mission2.output);
    conf.set("output", args[1] + "/" + output);
  }

  @Override
  protected void setupJob() {
    job.setMapperClass(TheMapper.class);
    job.setReducerClass(TheReducer.class);

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(TheValue.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
  }

  @Override
  protected String getDependecies() {
    return "2";
  }
}