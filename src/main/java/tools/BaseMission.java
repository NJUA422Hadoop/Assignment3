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
 * 规定了任务需要的一些功能，实现了一些重复的部分。
 * @author RailgunHamster（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/24
 */
public abstract class BaseMission {
  /**
   * 用户设置
   */
  protected Configuration conf;
  /**
   * 用户输入
   */
  protected String[] args;
  /**
   * 任务
   */
  protected Job job;
  /**
   * 主任务的实例
   */
  private Configured self;
  /**
   * 任务的控制类，用于多任务
   */
  private ControlledJob cjob;
  /**
   * 只初始化一次
   */
  private boolean init = false;
  /**
   * 本次所有的任务，用于添加任务间依赖
   */
  private BaseMission[] missions;
  /**
   * 用于打印log
   */
  private final Logger logger = Logger.getLogger(this.getClass());

  /**
   * 初始化
   * @param
   * Configured self 主任务实例。String[] args 用户输入参数
   */
  protected BaseMission(Configured self, String[] args) {
    this.conf = self.getConf();
    this.self = self;
    this.args = args;
  }

  /**
   * 获取ControlledJob。只会初始化一次
   * @return
   * isWorking()为false时，返回null。为true时，返回初始化完成的任务
   */
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
   * 设置所有任务的引用。用于依赖设置
   */
  final public void setupDependences(BaseMission[] missions) {
    this.missions = missions;
  }

  /**
   * 用于configuration值的设置。由子类实现
   * @need
   * 需要添加"input"以及"output"，用于此任务输入输出地址设置
   * @warn
   * 不能在这里调用getJob()
   */
  protected abstract void setupConf();
  /**
   * 这个函数用于job的设置。由子类实现。
   * @warn
   * 不能在这里调用getJob()
   */
  protected abstract void setupJob();

  /**
   * 在setupJob()之前初始化job。
   */
  private void beforeSetupJob() {
    try {
      job = Job.getInstance(conf, this.getClass().getSimpleName());
      job.setJarByClass(self.getClass());
    } catch(IOException ioe) {
      logger.error(ioe);
    }
  }

  /**
   * 在setupJob()之后设置controlled job，依赖，以及input、output path。
   */
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
   * 根据任务的getDependencies()返回的值，设置依赖的任务。
   */
  private void setupDependences() {
    for (Character c : getDependecies().toCharArray()) {
      int missionNumber = Character.getNumericValue(c) - 1;

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

  /**
   * 表示了此任务所依赖任务的序号。默认情况下为没有依赖
   * <pre>
   * Example:
   *  return "12" -> 此任务需要任务一、任务二完成后才会运行
   * </pre>
   * @warn
   * 字符串中数字不能超过missions的数组范围个数[1,N]
   */
  protected String getDependecies() {
    return "";
  }

  /**
   * 决定了该Mission是否可用。
   * 默认表示工作
   */
  public boolean isWorking() {
    return true;
  }
}
