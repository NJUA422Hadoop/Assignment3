package tools;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Tuple<A extends WritableComparable<? super A>, B extends WritableComparable<? super B>> implements WritableComparable<Tuple<A, B>> {
  public A first;
  public B second;

  public Tuple() {
    ;
  }

  public Tuple(A first, B second) {
    this.first = first;
    this.second = second;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
  }

  @Override
  public int compareTo(Tuple<A, B> o) {
    int a = this.first.compareTo(o.first);
    int b = this.second.compareTo(o.second);

    if (a != 0) {
      return a;
    } else {
      return b;
    }
  }
}