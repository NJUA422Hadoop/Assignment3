package mission1.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import org.ansj.library.DicLibrary;
// import org.ansj.library.StopLibrary;
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

    ClassLoader classLoader = AnsjLoader.class.getClassLoader();

    URL dic = classLoader.getResource("wuxia_name.txt");
    // URL stop = classLoader.getResource("stop_words.txt");
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(dic.openStream()));
      String line = null;
      while ((line = reader.readLine()) != null) {
        DicLibrary.insert(DicLibrary.DEFAULT, line, "nr", DicLibrary.DEFAULT_FREQ);
      }
      reader.close();
      /*
      reader = new BufferedReader(new InputStreamReader(stop.openStream()));
      line = null;
      while((line = reader.readLine()) != null) {
        StopLibrary.insertStopWords(StopLibrary.DEFAULT, line);
      }
      reader.close();
      */
    } catch(IOException ioe) {
      logger.error(ioe);
    }
  }
}