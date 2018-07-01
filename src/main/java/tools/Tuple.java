package tools;

/**
 * 一个Tuple
 * @author RailgunHamster（王宇鑫 151220114）
 * @version 1.0
 * @date 2018/6/27
 */
public class Tuple<A, B> {
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
  public String toString() {
    return String.format("<%s,%s>", first.toString(), second.toString());
  }
}