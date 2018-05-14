import hbase.HBase;

/**
 * 单机任务
 * 参数：任务名称
 * @author RailgunHamster
 * @version 1.1
 * @date 2018/5/14
 */
public class Mission {
    public void Wuxia() {
        HBase hbase = new HBase();
        hbase.saveToLocal("Wuxia");
    }

    public void defaultMission() {
    }

    public void run(String[] args) {
        if (args.length != 1) {
            return;
        }

        switch(args[0]) {
            case "Wuxia": Wuxia(); break;
            default: defaultMission();
        }
    }

    public static void main(String[] args) {
        new Mission().run(args);
    }
}