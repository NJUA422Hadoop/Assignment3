package mission1.tools;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Tuple&lt;String, Long&gt;
 * @author RailgunHasmter（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/28
 */
public class TheKey implements WritableComparable<TheKey> {
  public String first;
  public Long second;

  public TheKey(String first, Long second) {
    this.first = first;
    this.second = Long.valueOf(second);
  }

  public TheKey(TheKey old) {
    this.first = old.first;
    this.second = Long.valueOf(old.second);
  }

  public TheKey() {
    ;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readUTF();
    second = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(first);
    out.writeLong(second);
  }

  @Override
  public int compareTo(TheKey o) {
    int a = this.first.compareTo(o.first);
    int b = this.second.compareTo(o.second);

    if (a != 0) {
      return a;
    } else {
      return b;
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj instanceof TheKey) {
      TheKey _obj = (TheKey) obj;
      return first.equals(_obj.first) && second.equals(_obj.second);
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return String.format("<%s,%s>", first, second);
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}