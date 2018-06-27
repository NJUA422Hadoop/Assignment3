package mission1.mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
   * 存储资源
   */
  private Map<String, List<String>> sources = new HashMap<>();

  /**
   * load source
   */
  private void loadSource(String path) {
    ClassLoader classLoader = AnsjLoader.class.getClassLoader();
    URL dic = classLoader.getResource(path);

    List<String> list = new ArrayList<>();
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(dic.openStream()));
      String line = null;
      while ((line = reader.readLine()) != null) {
        list.add(line);
      }
      reader.close();
    } catch(IOException ioe) {
      logger.error(ioe);
    }

    sources.put(path, list);
  }

  /**
   * 获取资源列表
   */
  public List<String> getSource(String path) {
    if (!sources.containsKey(path)) {
      loadSource(path);
    }

    return sources.get(path);
  }

  /**
   * load ansj_seg dictionary
   */
  public void ENVload() {
    if (init) {
      return;
    } else {
      init = true;
    }

    for (String name : getSource("wuxia_name.txt")) {
      DicLibrary.insert(DicLibrary.DEFAULT, name);
    }
  }
}