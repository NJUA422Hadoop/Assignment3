package mission7;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import mission7.mapper.PageRankSortMapper;

import tools.BaseMission;

/**
 * 第六个任务
 * 分析结果整理
    （1）PageRank值进行排序
    （2）将属于同一个标签的人物输出到一起
 *  wyd
 */

public class Mission7 extends BaseMission {
  public static final String input = "???";
  public static final String output = "???";

  public Mission7(Configured self, String[] args) {
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
    job.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);
    job.setSortComparatorClass(PageRankSortMapper.FloatComparator.class);
    job.setMapperClass(PageRankSortMapper.class);
    // set mapper class ... etc
    return job;
  }

  @Override
  protected String getDependecies() {
    // 依赖任务五
    return "5";
  }

  @Override
  public boolean isWorking() {
    return false;
  }
}
