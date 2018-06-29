import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.log4j.Logger;

/**
 * 单机任务
 * 参数：任务名称
 * @author RailgunHamster
 * @version 2.0
 * @date 2018/6/22
 */
public class SingleMission {
  public void wuxia(String savePath) {
    return;
  }

  public void ansj_seg() {
    System.out.println(ToAnalysis.parse("他这个人性情不定，只是有点怕麻烦。"));
  }

  /**
   * @return 是否成功
   */
  public boolean run(String[] args) {
    if (args.length == 0) {
      return false;
    }

    switch(args[0]) {
      case "wuxia":
        if (args.length != 2) {
          return false;
        }
        wuxia(args[1]);
      case "ansj_seg":
        if (args.length != 1) {
          return false;
        }
        ansj_seg();
      break;
        default: return false;
    }

    return true;
  }

  /**
   * log4j
   */
  private static final Logger logger = Logger.getLogger(SingleMission.class);

  /**
   * 所有参数均不对，打印错误信息
   */
  public static void defaultMission() {
    logger.error("参数格式不正确，正确格式：\n\tcommand/mission.bat(sh) ${MissionName} [${OutputFilePath}]");
  }

  public static void main(String[] args) {
    if (!new SingleMission().run(args)) {
      SingleMission.defaultMission();
    }
  }
}