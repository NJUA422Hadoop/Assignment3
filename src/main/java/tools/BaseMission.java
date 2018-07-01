package tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
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
   * 主任务的实例
   */
  private Configured self;
  /**
   * 任务
   */
  private List<Job> jobs = new ArrayList<>();
  /**
   * 任务的控制类，用于多任务
   */
  private List<ControlledJob> cjobs = new ArrayList<>();
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
   * @param self 主任务实例
   * @param args 用户输入参数
   */
  protected BaseMission(Configured self, String[] args) {
    this.conf = self.getConf();
    this.self = self;
    this.args = args;
  }

  /**
   * 获取ControlledJob。只会初始化一次
   * @return isWorking()为false时，返回null。为true时，返回初始化完成的任务
   */
  final public List<ControlledJob> getJobs() {
    if (isWorking()) {
      initialize();
    }
    
    return cjobs;
  }

  /**
   * 第一次getJobs()时进行的初始化
   */
  private void initialize() {
    if (init) {
      return;
    }

    init = true;

    for (int i = 1;i <= times();i++) {
      setupConf(i);
      boolean fail = !afterSetupJob(setupJob(beforeSetupJob(i), i));

      // 如果失败，说明输出文件夹已存在，删除任务，等待下一个任务即可。
      if (fail) {
        jobs.remove(jobs.size() - 1);
        cjobs.remove(cjobs.size() - 1);
      }
    }
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
   * 不能在这里调用getJobs()
   * @param index 表示这是第几次执行该任务
   */
  protected abstract void setupConf(int index);
  /**
   * 这个函数用于job的设置。由子类实现
   * @warn
   * 不能在这里调用getJobs()
   * @param job 任务
   * @param index 任务序号
   * @return job 设置后的任务
   */
  protected abstract Job setupJob(Job job, int index);

  /**
   * 在setupJob()之前初始化job
   * @param index 任务序号
   * @return job 设置后的job
   */
  private Job beforeSetupJob(int index) {
    try {
      Job job = Job.getInstance(conf, this.getClass().getSimpleName() + "_" + index);
      job.setJarByClass(self.getClass());

      jobs.add(job);

      return job;
    } catch(IOException ioe) {
      logger.error(ioe);

      return null;
    }
  }

  /**
   * 在setupJob()之后设置controlled job，依赖，以及input、output path
   */
  private boolean afterSetupJob(Job job) {
    try {
      ControlledJob cjob = new ControlledJob(conf);
      cjob.setJob(job);

      cjobs.add(cjob);

      setupDependences();

      Path input = new Path(conf.get("input"));
      Path output = new Path(conf.get("output"));

      if (FileSystem.newInstance(conf).exists(output)) {
        return false;
      }

      FileInputFormat.addInputPath(job, input);
      FileOutputFormat.setOutputPath(job, output);

      return true;
    } catch(IOException ioe) {
      logger.error(ioe);

      return false;
    }
  }

  /**
   * 根据任务的getDependencies()返回的值，设置依赖的任务
   */
  private void setupDependences() {
    int size = jobs.size();

    if (size < 1) {
      return;
    }

    if (size == 1) {
      for (Character c : getDependecies().toCharArray()) {
        if (!Character.isDigit(c)) {
          continue;
        }

        int missionNumber = Character.getNumericValue(c) - 1;

        if (missionNumber < 0 || missionNumber >= missions.length) {
          continue;
        }

        List<ControlledJob> _cjobs = missions[missionNumber].getJobs();

        if (_cjobs.size() == 0) {
          continue;
        }

        cjobs.get(0).addDependingJob(_cjobs.get(_cjobs.size() - 1));
      }
    } else {
      cjobs.get(size - 1).addDependingJob(cjobs.get(size - 2));
    }
  }

  /**
   * 规定了该任务重复的次数。默认情况一次
   */
  protected int times() {
    return 1;
  }

  /**
   * 表示了此任务所依赖任务的序号。默认情况下为没有依赖
   * <pre>
   * Example:
   *  return "12" -> 此任务需要任务一、任务二完成后才会运行
   * </pre>
   * 字符串中不符合规定的字符会被无视
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

  /**
   * 将baseMissions中所有任务添加至JobControl
   */
  public static void addAll(BaseMission[] baseMissions, JobControl jobControl) {
    for (int i = 0;i < baseMissions.length;i++) {
      for (ControlledJob job : baseMissions[i].getJobs()) {
        jobControl.addJob(job);
      }
    }
  }

  /**
   * 将baseMissions中的所有任务添加相应的依赖
   */
  public static void addDependencies(BaseMission[] baseMissions) {
    for (int i = 0;i < baseMissions.length;i++) {
      baseMissions[i].setupDependences(baseMissions);
    }
  }
}
