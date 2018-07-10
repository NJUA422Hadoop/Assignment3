package mission5.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TheValueTest {
  private TheValue tvalue = new TheValue();

  @Test
  public void toStrTest() {
    tvalue.set("[狄云:0.25|戚长发:0.25|戚芳:0.5]");
    assertEquals("[狄云:0.25|戚长发:0.25|戚芳:0.5]", tvalue.toString());
  }

  @Test
  public void maxTest() {
    tvalue.set("[狄云:0.25|戚长发:0.25|戚芳:0.5]");
    assertEquals("戚芳", tvalue.max());
  }

  @Test
  public void addLabelTest() {
    Map<String, String> map = new HashMap<>();

    map.put("戚芳", "qf");
    map.put("戚长发", "qcf");
    map.put("狄云", "dy");

    tvalue.set("[狄云:0.25|戚长发:0.25|戚芳:0.5]");
    tvalue.addLabel(map);

    assertEquals("[狄云,dy:0.25|戚长发,qcf:0.25|戚芳,qf:0.5]", tvalue.toString());
  }
}