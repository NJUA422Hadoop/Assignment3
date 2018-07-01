package tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * 加载所需资源
 * @author RailgunHamster（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/30
 */
public class Loader {
  /**
   * 用class初始化，用于debug。
   */
  public Loader(Class<?> clazz) {
    logger = Logger.getLogger(clazz);
  }
  /**
   * 打印log
   */
  private final Logger logger;
  /**
   * 存储资源，key为地址path，value为资源每行内容的List<String>
   */
  private Map<String, List<String>> sources = new HashMap<>();
  /**
   * 从jar包内，加载path处的资源到Mpa中
   * @param path jar包内资源的地址
   */
  private void loadSource(String path) {
    ClassLoader classLoader = Loader.class.getClassLoader();
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
   * 获取资源列表，并返回
   * @param path jar包内资源的地址
   * @return 该资源的List<String>
   */
  public List<String> getSource(String path) {
    if (!sources.containsKey(path)) {
      loadSource(path);
    }

    return sources.get(path);
  }
}