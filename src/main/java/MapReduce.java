import mission1.Mission1;
import mission2.Mission2;
import mission3.Mission3;
import mission4.Mission4;
import mission5.Mission5;
import mission6.Mission6;

import tools.BaseMission;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * InvertedIndexAssignment for 武侠
 * @author RailgunHamster (王宇鑫 151220114)
 * @version 2.0
 * @date 2018/6/22
 */
public class MapReduce extends Configured implements Tool {
  @Override
  public int run(String[] args) throws Exception {
    BaseMission[] missions = {
      new Mission1(this, args),
      new Mission2(this, args),
      new Mission3(this, args),
      new Mission4(this, args),
      new Mission5(this, args),
      new Mission6(this, args),
    };

    // 设置依赖
    for (int i = 0;i < missions.length;i++) {
      missions[i].setupDependences(missions);
    }

    // 添加job
    JobControl jobControl = new JobControl("Wuxia");
    for (int i = 0;i < missions.length;i++) {
      jobControl.addJob(missions[i].getJob());
    }

    // run
    Thread runner = new Thread(jobControl);
    runner.start();

    // wait for end
    while(true) {
      if (jobControl.allFinished()) {
        System.out.println(jobControl.getSuccessfulJobList());
        jobControl.stop();
        return 0;
      }
    }
  }

  public static void main(String[] args) {
    try {
      System.exit(ToolRunner.run(new MapReduce(), args));
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
