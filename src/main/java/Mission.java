import org.ansj.splitWord.analysis.ToAnalysis;

import hbase.HBase;
import reducer.InvertedIndexReducer;

/**
 * 单机任务
 * 参数：任务名称
 * @author RailgunHamster
 * @version 2.0
 * @date 2018/6/22
 */
public class Mission {
    public void Wuxia(String savePath) {
        HBase hbase = HBase.shared;
        hbase.saveToLocal(
            "Wuxia",
            savePath,
            InvertedIndexReducer.columnFamily,
            InvertedIndexReducer.column
        );
    }

    /**
     * @return successful or not
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
     * print info when mission failed
     */
    public static void defaultMission() {
        String info = "参数格式不正确，正确格式：\n\tcommand/mission.bat(sh) ${MissionName} ${OutputFilePath}";
        System.out.println(info);
    }

    public static void main(String[] args) {
        System.out.println("ansj_seg Test: ");
        System.out.println(ToAnalysis.parse("我叫王宇鑫，who are you？"));
        if (!new Mission().run(args)) {
            Mission.defaultMission();
        }
    }
}