import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.ansj.library.DicLibrary;
import org.apache.log4j.Logger;

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
        Mission.logger.debug(info);
    }

    /**
     * log4j
     */
    private static Logger logger = Logger.getLogger(Mission.class);

    public static void main(String[] args) {
        // load
        Mission.ENVLoad();
        // run
        if (!new Mission().run(args)) {
            Mission.defaultMission();
        }
    }

    /**
     * load... ansj_seg dictionary
     */
    public static void ENVLoad() {
        URL dic = Mission.class.getClassLoader().getResource("wuxia_name.txt");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(dic.openStream()));
            String name = null;
            while ((name = reader.readLine()) != null) {
                DicLibrary.insert(DicLibrary.DEFAULT, name, "nr", DicLibrary.DEFAULT_FREQ);
            }
            reader.close();
        } catch(IOException ioe) {
            Mission.logger.error(ioe);
        }
    }
}