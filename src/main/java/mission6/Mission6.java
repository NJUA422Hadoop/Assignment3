package mission6;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;

import mission6.mapper.PageRankSortMapper;

import tools.BaseMission;

/**
 * 第六个任务
 * 做这个任务的人写一下描述：
 *  XXX
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
    job.setJarByClass(PageRankSort.class);
    jJob.setOutputKeyClass(FloatWritable.class);
    job.setOutputValueClass(Text.class);
    job.setSortComparatorClass(PageRankSort.FloatComparator.class);
    job.setMapperClass(PageRankSort.PageRankSortMapper.class);
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