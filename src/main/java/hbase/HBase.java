package hbase;

import tools.Runner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.Logger;

/**
 * 之后对HBase的操作，就在这个类中实现，或者在同层的hbase包中实现，然后在这个类中提供接口。
 * @author RailgunHamster
 * @version 1.1
 * @date 2018/5/14
 */
public class HBase {
  Configuration conf = HBaseConfiguration.create();

  private final Logger logger = Logger.getLogger(HBase.class);

  /**
   * 获得唯一实例
   */
  public static final HBase shared = new HBase();
  private HBase() {
    set(this.conf);
  }

  /**
   * setting
   */
  public static void set(Configuration conf) {
    conf.set("hbase.zookeeper.quorum", "localhost");
  }

  /**
   * create table
   * @author RailgunHamster
   */
  public void createTable(String table, String columnFamily) {
    try {
      conf.set("table", table);
      conf.set("columnFamily", columnFamily);
      Runner mission = new CreateTable(conf);
      mission.start();
    } catch(IOException ioe) {
      logger.error(ioe);
    }
  }

  /**
   * save from hbase to local
   * @author WaterYe
   */
  public void saveToLocal(String table, String savePath, String columnFamily, String column) {
    try {
      conf.set("table", table);
      conf.set("savePath", savePath);
      conf.set("columnFamily", columnFamily);
      conf.set("column", column);
      Runner mission = new SaveTableToLocal(conf);
      mission.start();
    } catch(IOException ioe) {
      logger.error(ioe);
    }
  }

  /**
   * ?
   */
  public void Sort() {
    try {
      Runner mission = new Sorter(conf);
      mission.start();
    } catch(IOException ioe) {
      logger.error(ioe);
    }
  }

  /**
   * get Hbase configuration
   */
  public Configuration getConf() {
    return this.conf;
  }
}