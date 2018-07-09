package mission6;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;

import tools.BaseMission;

/**
 * 第六个任务
 * 分析结果整理
    （1）PageRank值进行排序
    （2）将属于同一个标签的人物输出到一起
 *  wyd
 */

public class Mission6 extends BaseMission {
  public static final String input = "???";
  public static final String output = "???";

  public Mission6(Configured self, String[] args) {
    super(self, args);
  }

  @Override
  protected void setupConf(int index) {
    conf.set("input", input); // or args[?]
    conf.set("output", output);
    // more settings
  }

  @Override
  protected Job setupJob(Job job, int index) {
    // set mapper class ... etc
    return job;
  }

  @Override
  protected String getDependecies() {
    // 依赖任务四、五
    return "45";
  }

  @Override
  public boolean isWorking() {
    return false;
  }
}
