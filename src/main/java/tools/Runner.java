package tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * 用于启动任务。
 * @author RailgunHamster（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/7
 */
public abstract class Runner {
  protected final Configuration conf;

  protected Runner(Configuration conf) {
    this.conf = conf;
  }

  /**
   * 供用户调用，不可重写
   */
  final public void start() throws IOException {
    pre();
    run();
    end();
  }

  protected void pre() throws IOException {}
  protected abstract void run() throws IOException;
  protected void end() throws IOException {}
}