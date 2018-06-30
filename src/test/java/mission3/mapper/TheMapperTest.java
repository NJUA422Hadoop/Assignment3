package mission3.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TheMapperTest {
  @Test
  public void splitTest() {
    assertEquals("<戚芳,<卜垣,2>>", new TheMapper().split(new Text("<戚芳,卜垣>"), new Text("2")).toString());
  }
}