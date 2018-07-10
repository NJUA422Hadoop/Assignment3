package mission6;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

import mission4.Mission4;
import mission6.mapper.PageRankSortMapper;
import mission6.reducer.PageRankSortReducer;

import tools.BaseMission;

/**
 * 第六个任务:
 * <pre>
 * 基于PageRank值的排序
 * 输入: Mission4的输出，格式如下
 * 狄云   0.8755596222029398  [戚芳:0.33333|戚长发:0.333333|卜垣:0.333333]
 * 戚芳   1.1244404174153242  [狄云:0.25|戚长发:0.25|卜垣:0.5]
 * 戚长发 0.8755596222029398  [狄云:0.33333|戚芳:0.333333|卜垣:0.333333]
 * 卜垣   1.1244404174153242  [狄云:0.25|戚芳:0.5|戚长发:0.25]
 * 输出: 排好序的人物的PageRank值
 * 卜垣   1.1244404174153242
 * 戚芳   1.1244404174153242
 * 戚长发 0.8755596222029398
 * 狄云   0.8755596222029398
 * </pre>
 * @author WaterYe（王尧 151220112）
 * @version 1.0
 * @date 2018/7/9
 */

public class Mission6 extends BaseMission {
  public static final String input = "mission4";
  public static final String output = "mission6";

  public Mission6(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf(int index) {
    conf.set("input", args[1] + "/" + Mission4.output + "/30"); // or args[?]
    conf.set("output", args[1] + "/" + output);
    // more settings
  }

  @Override
  protected Job setupJob(Job job, int index) {
    job.setMapperClass(PageRankSortMapper.class);
    job.setReducerClass(PageRankSortReducer.class);
    job.setSortComparatorClass(PageRankSortMapper.FloatComparator.class);
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);
    // set mapper class ... etc
    return job;
  }

  @Override
  protected String getDependecies() {
    // 依赖任务四
    return "4";
  }

  @Override
  public boolean isWorking() {
    return true;
  }
}
