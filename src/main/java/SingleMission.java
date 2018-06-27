/**
 * 单机任务
 * 参数：任务名称
 * @author RailgunHamster
 * @version 2.0
 * @date 2018/6/22
 */
public class SingleMission {
  public void Wuxia(String savePath) {
    return;
  }

  /**
   * @return 是否成功
   */
  public boolean run(String[] args) {
    if (args.length == 0) {
      return false;
    }

    switch(args[0]) {
      case "Wuxia":
        if (args.length != 2) {
          return false;
        }
        Wuxia(args[1]);
      break;
        default: return false;
    }

    return true;
  }

  /**
   * 所有参数均不对，打印错误信息
   */
  public static void defaultMission() {
    String info = "参数格式不正确，正确格式：\n\tcommand/mission.bat(sh) ${MissionName} ${OutputFilePath}";
    System.out.println(info);
  }

  public static void main(String[] args) {
    if (!new SingleMission().run(args)) {
      SingleMission.defaultMission();
    }
  }
}