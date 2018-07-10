
package mission5.tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.io.WritableComparable;

import tools.Tuple;

/**
 * @author RailgunHasmter（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/7/10
 */
public class TheValue implements WritableComparable<TheValue> {
  private List<Tuple<String, Double>> list = new ArrayList<>();

  public TheValue(TheValue old) {
    set(old);
  }

  public TheValue(List<Tuple<String, Double>> values) {
    set(values);
  }

  public TheValue(String str) {
    set(str);
  }

  public TheValue() {
    clear();
  }

  public void addLabel(Map<String, String> map) {
    for (Tuple<String, Double> t : list) {
      t.first = String.format("%s,%s", t.first, map.getOrDefault(t.first, t.first));
    }
  }

  public void set(TheValue old) {
    clear();

    if (old == null) {
      return;
    }

    for (Tuple<String, Double> t : old.list) {
      list.add(new Tuple<String, Double>(t.first, Double.valueOf(t.second)));
    }
  }

  public void set(List<Tuple<String, Double>> values) {
    clear();

    if (values == null) {
      return;
    }

    for (Tuple<String, Double> t : values) {
      list.add(new Tuple<String, Double>(t.first, Double.valueOf(t.second)));
    }
  }

  public void set(String str) {
    clear();

    if (str == null) {
      return;
    }

    String[] tuples = str.substring(1, str.length() - 1).split("\\|");
    for (String tuple : tuples) {
      String[] _tuple = tuple.split(":");
      list.add(new Tuple<>(_tuple[0], Double.valueOf(_tuple[1])));
    }
  }

  public void clear() {
    list.clear();
  }

  public int size() {
    return list.size();
  }

  public String max() {
    Tuple<String, Double> max = new Tuple<>("", -1.0);
    for (Tuple<String, Double> t : list) {
      if (t.second > max.second) {
        max = t;
      }
    }

    // random
    List<String> names = new ArrayList<>();
    for (Tuple<String, Double> t : list) {
      if (t.second == max.second) {
        names.add(t.first);
      }
    }

    Random random = new Random(System.currentTimeMillis());
    return names.get(random.nextInt(names.size()));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    clear();
    // read length
    int length = in.readInt();
    // read
    for (int i = 0;i < length;i++) {
      list.add(new Tuple<>(in.readUTF(), in.readDouble()));
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // write length
    out.writeInt(size());
    // write
    for (Tuple<String, Double> t : list) {
      out.writeUTF(t.first);
      out.writeDouble(t.second);
    }
  }

  @Override
  public int compareTo(TheValue o) {
    Integer msize = this.size();
    Integer osize = o.size();

    if (msize > osize) {
      return 1;
    } else if (msize < osize) {
      return -1;
    } else {
      for (int i = 0;i < msize;i++) {
        Tuple<String, Double> mt = this.list.get(i);
        Tuple<String, Double> ot = o.list.get(i);
        int first = mt.first.compareTo(ot.first);
        int second = mt.second.compareTo(ot.second);

        if (first == 0 && second == 0) {
          continue;
        } else if (first == 0) {
          return second;
        } else {
          return first;
        }
      }

      return 0;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj instanceof TheValue) {
      TheValue _obj = (TheValue) obj;
      return this.compareTo(_obj) == 0;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('[');
    int size = size() - 1;
    for (int i = 0;i <= size;i++) {
      Tuple<String, Double> t = list.get(i);
      sb.append(t.first);
      sb.append(':');
      sb.append(t.second);
      if (i != size) {
        sb.append('|');
      }
    }
    sb.append(']');
    return sb.toString();
  }
}