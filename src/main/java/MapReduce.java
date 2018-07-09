import mission1.Mission1;
import mission2.Mission2;
import mission3.Mission3;
import mission4.Mission4;
import mission5.Mission5;
import mission6.Mission6;
import mission7.Mission7;

import tools.BaseMission;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Assignment for 武侠
 * @author RailgunHamster (王宇鑫 151220114)
 * @version 2.0
 * @date 2018/6/22
 */
public class MapReduce extends Configured implements Tool {
  /**
   * log4j
   */
  private static final Logger logger = Logger.getLogger(MapReduce.class);

  @Override
  public int run(String[] args) throws Exception {
    BaseMission[] missions = {
      new Mission1(this, args),
      new Mission2(this, args),
      new Mission3(this, args),
      new Mission4(this, args),
      new Mission5(this, args),
      new Mission6(this, args),
      new Mission7(this, args),
    };

    // 设置依赖，注意：请按照拓扑序进行setup，保证无环，否则会造成死循环
    BaseMission.addDependencies(missions);

    // 添加job
    JobControl jobControl = new JobControl("wuxia");
    BaseMission.addAll(missions, jobControl);

    // 添加线程
    Thread runner = new Thread(jobControl);
    runner.start();

    // 运行，等待结束
    while (true) {
      if (jobControl.allFinished()) {
        logger.info(jobControl.getSuccessfulJobList());
        jobControl.stop();
        return 0;
      }
    }
  }

  public static void main(String[] args) {
    try {
      System.exit(ToolRunner.run(new MapReduce(), args));
    } catch(Exception e) {
      logger.error(e);
    }
  }
}
