package mission2.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Test;

public class TheKeyTest {
  @Test
  public void test() {
    TheKey key1 = new TheKey("卜垣", "戚芳");
    TheKey key2 = new TheKey("卜垣", "戚芳");
    assertEquals(key1.hashCode(), key2.hashCode());
  }
}