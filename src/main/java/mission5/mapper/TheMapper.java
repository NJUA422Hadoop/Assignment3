package mission5.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mission5.tools.TheValue;

public class TheMapper extends Mapper<Text, Text, Text, TheValue> {
  /**
   * 考虑这次map过程中不影响本次对label的搜索。采用两个map。
   */
  public static final boolean twoMaps = true;

  private TheValue tvalue = new TheValue();
  private Text text = new Text();
  private boolean _final = false;
  private Map<String, String> inmap = new HashMap<>();
  private Map<String, String> outmap = new HashMap<>();

  @Override
  protected void map(Text key, Text value, Mapper<Text, Text, Text, TheValue>.Context context)
      throws IOException, InterruptedException {
    String name = key.toString();
    tvalue.set(value.toString());
    if (_final) {
      name = String.format("%s\t%s", name, inmap.getOrDefault(name, name));
      tvalue.addLabel(inmap);
    } else {
      String label = tvalue.max();
      outmap.put(key.toString(), inmap.getOrDefault(label, label));
    }
    text.set(name);
    context.write(text, tvalue);
  }

  private Configuration conf;
  private FileSystem fs;

  private static final String utf8 = "UTF-8";
  public static final String delimeter = "\t";
  public static final String newline = "\n";
  private static byte[] _delimeter;
  private static byte[] _newline;
  public static final String tempPath = "/_temp/labels.txt";

  @Override
  protected void setup(Mapper<Text, Text, Text, TheValue>.Context context) throws IOException, InterruptedException {
    conf = context.getConfiguration();
    fs = FileSystem.newInstance(conf);

    if (!twoMaps) {
      outmap = inmap;
    }

    if (conf.get("final") == "Y") {
      _final = true;
    }

    Path inPath = new Path(conf.get("input") + tempPath);

    if (!fs.exists(inPath)) {
      return;
    }

    FSDataInputStream in = fs.open(inPath);

    byte[] buffer = new byte[in.available()];

    in.read(buffer);
    Text t = new Text(buffer);

    String[] tuples = t.toString().split(TheMapper.newline);
    
    for (String tuple : tuples) {
      String[] split = tuple.split(TheMapper.delimeter);
      String name = split[0];
      String label = split[1];
      inmap.put(name, label);
    }

    in.close();
  }

  @Override
  protected void cleanup(Mapper<Text, Text, Text, TheValue>.Context context) throws IOException, InterruptedException {
    if (_final) {
      return;
    }

    if (_delimeter == null) {
      _delimeter = delimeter.getBytes(utf8);
    }
    if (_newline == null) {
      _newline = newline.getBytes(utf8);
    }

    FSDataOutputStream out = fs.create(new Path(conf.get("output") + tempPath));

    for (Map.Entry<String, String> t : outmap.entrySet()) {
      out.write(t.getKey().getBytes(utf8));
      out.write(_delimeter);
      out.write(t.getValue().getBytes(utf8));
      out.write(_newline);
    }

    out.close();
    fs.close();
  }
}