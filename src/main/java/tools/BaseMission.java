package tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

/**
 * 规定了任务需要的一些功能
 */
public abstract class BaseMission {
  protected Configuration conf;
  private Configured self;
  protected String[] args;
  protected Job job;
  private ControlledJob cjob;
  private boolean init = false;

  protected BaseMission(Configured self, String[] args) {
    this.conf = self.getConf();
    this.self = self;
    this.args = args;
  }

  public ControlledJob getJob() {
    if (!isWorking()) {
      return null;
    }

    if (!init) {
      init = true;
      setupConf();
      beforeSetupJob();
      setupJob();
    }
    
    return this.cjob;
  }

  /**
   * 这个函数用于configuration值的设置
   * 不能在这里调用getJob()
   */
  protected abstract void setupConf();
  /**
   * 这个函数用于job的设置
   * 不能在这里调用getJob()
   */
  protected abstract void setupJob();

  private void beforeSetupJob() {
    try {
      Class<?> clazz = self.getClass();
      job = Job.getInstance(conf, clazz.getSimpleName());
      job.setJarByClass(clazz);

      FileInputFormat.addInputPath(job, new Path(conf.get("input")));
      FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

      cjob = new ControlledJob(conf);
      cjob.setJob(job);
    } catch(IOException ioe) {
      ioe.printStackTrace();
    }
  }

  /**
   * 设置依赖
   */
  protected String getDependecies() {
    return "";
  }

  final public boolean setupDependences(BaseMission[] missions) {
    if (this.cjob == null) {
      return false;
    }

    String jobNames = getDependecies();

    for (int i = 0;i < jobNames.length();i++) {
      int missionNumber = Character.getNumericValue(jobNames.charAt(i)) - 1;

      if (missionNumber < 0 || missionNumber >= missions.length) {
        continue;
      }

      ControlledJob _cjob = missions[missionNumber].getJob();

      if (_cjob == null) {
        continue;
      }

      this.cjob.addDependingJob(_cjob);
    }

    return true;
  }

  /**
   * 决定了该Mission是否可用，如需修改，override。
   */
  public boolean isWorking() {
    return true;
  }
}
