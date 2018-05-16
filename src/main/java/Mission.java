import hbase.HBase;
import reducer.InvertedIndexReducer;

/**
 * 单机任务
 * 参数：任务名称
 * @author RailgunHamster
 * @version 1.1
 * @date 2018/5/14
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

    public void defaultMission() {
    }

    public void run(String[] args) {
        if (args.length == 0) {
            return;
        }

        switch(args[0]) {
            case "Wuxia":
                if (args.length != 2) {
                    return;
                }
                Wuxia(args[1]);
            break;
            default: defaultMission();
        }
    }

    public static void main(String[] args) {
        new Mission().run(args);
    }
}