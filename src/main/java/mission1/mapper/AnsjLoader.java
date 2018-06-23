package mission1.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.ansj.library.DicLibrary;
import org.apache.log4j.Logger;

/**
 * 加载Ansj_seg库，添加本任务所需词语
 * @author RailgunHamster（王宇鑫 151220114）
 * @date 2018/6/24
 */
public class AnsjLoader {
  // 唯一实例
  public final static AnsjLoader shared = new AnsjLoader();
  private AnsjLoader() {}

  /**
   * logger
   */
  private Logger logger = Logger.getLogger(AnsjLoader.class);

  /**
   * 只load一次
   */
  private boolean init = false;

  /**
   * load ansj_seg dictionary
   */
  public void ENVload() {
    if (init) {
      return;
    } else {
      init = true;
    }

    URL dic = AnsjLoader.class.getClassLoader().getResource("wuxia_name.txt");
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(dic.openStream()));
      String name = null;
      while ((name = reader.readLine()) != null) {
        DicLibrary.insert(DicLibrary.DEFAULT, name, "nr", DicLibrary.DEFAULT_FREQ);
      }
      reader.close();
    } catch(IOException ioe) {
      logger.error(ioe);
    }
  }
}