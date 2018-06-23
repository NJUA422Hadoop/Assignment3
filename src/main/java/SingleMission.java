import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.ansj.library.DicLibrary;
import org.apache.log4j.Logger;

import hbase.HBase;

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
        SingleMission.logger.debug(info);
    }

    /**
     * log4j
     */
    private static Logger logger = Logger.getLogger(SingleMission.class);

    public static void main(String[] args) {
        // load
        SingleMission.ENVLoad();
        // run
        if (!new SingleMission().run(args)) {
            SingleMission.defaultMission();
        }
    }

    /**
     * load... ansj_seg dictionary
     */
    public static void ENVLoad() {
        URL dic = SingleMission.class.getClassLoader().getResource("wuxia_name.txt");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(dic.openStream()));
            String name = null;
            while ((name = reader.readLine()) != null) {
                DicLibrary.insert(DicLibrary.DEFAULT, name, "nr", DicLibrary.DEFAULT_FREQ);
            }
            reader.close();
        } catch(IOException ioe) {
            SingleMission.logger.error(ioe);
        }
    }
}