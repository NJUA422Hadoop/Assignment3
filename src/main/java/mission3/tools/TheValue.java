package mission3.tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * Tuple&lt;String, Int&gt;
 * @author RailgunHasmter（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/28
 */
public class TheValue implements WritableComparable<TheValue> {
  public String first;
  public Integer second;

  public TheValue(String first, Integer second) {
    this.first = first;
    this.second = Integer.valueOf(second);
  }

  public TheValue(TheValue old) {
    this.first = old.first;
    this.second = Integer.valueOf(old.second);
  }

  public TheValue() {
    ;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readUTF();
    second = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(first);
    out.writeInt(second);
  }

  @Override
  public int compareTo(TheValue o) {
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
    } else if (obj instanceof TheValue) {
      TheValue _obj = (TheValue) obj;
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