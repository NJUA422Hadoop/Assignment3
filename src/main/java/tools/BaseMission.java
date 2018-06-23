package tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

/**
 * 规定了任务需要的一些功能
 */
public abstract class BaseMission {
  protected Configuration conf;
  private Configured self;
  protected String[] args;
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

  protected abstract void setupConf();
  protected abstract void setupJob();

  private void beforeSetupJob() {
    try {
      Class<?> clazz = self.getClass();
      Job job = Job.getInstance(conf, clazz.getSimpleName());
      job.setJarByClass(clazz);

      ControlledJob cjob = new ControlledJob(conf);
      cjob.setJob(job);

      this.cjob = cjob;
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

  final public void setupDependences(BaseMission[] missions) {
    String jobNames = getDependecies();

    for (int i = 0;i < jobNames.length();i++) {
      int missionNumber = Character.getNumericValue(jobNames.charAt(i)) - 1;
      if (missionNumber < 0 && missionNumber >= missions.length) {
        continue;
      }
      this.cjob.addDependingJob(missions[missionNumber].getJob());
    }
  }

  /**
   * 决定了该Mission是否可用，如需修改，override。
   */
  public boolean isWorking() {
    return true;
  }
}
