package tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import org.apache.log4j.Logger;

/**
 * 规定了任务需要的一些功能
 */
public abstract class BaseMission {
  protected Configuration conf;
  protected String[] args;
  protected Job job;

  private Configured self;
  private ControlledJob cjob;
  private boolean init = false;
  private BaseMission[] missions;

  private Logger logger = Logger.getLogger(this.getClass());

  protected BaseMission(Configured self, String[] args) {
    this.conf = self.getConf();
    this.self = self;
    this.args = args;
  }

  final public ControlledJob getJob() {
    if (!isWorking()) {
      return null;
    }

    if (!init) {
      init = true;
      setupConf();
      beforeSetupJob();
      setupJob();
      afterSetupJob();
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
      job = Job.getInstance(conf, this.getClass().getSimpleName());
      job.setJarByClass(self.getClass());
    } catch(IOException ioe) {
      logger.error(ioe);
    }
  }

  private void afterSetupJob() {
    try {
      cjob = new ControlledJob(conf);
      cjob.setJob(job);

      setupDependences();

      FileInputFormat.addInputPath(job, new Path(conf.get("input")));
      FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));
    } catch(IOException ioe) {
      logger.error(ioe);
    }
  }

  /**
   * 设置依赖
   */
  protected String getDependecies() {
    return "";
  }

  private void setupDependences() {
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
  }

  public void setupDependences(BaseMission[] missions) {
    this.missions = missions;
  }

  /**
   * 决定了该Mission是否可用，如需修改，override。
   */
  public boolean isWorking() {
    return true;
  }
}
