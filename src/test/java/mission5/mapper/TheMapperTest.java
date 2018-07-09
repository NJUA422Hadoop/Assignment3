package mission5.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Test;

public class TheMapperTest {
  @Test
  public void findlabelTest() {
    assertEquals("b", TheMapper.findlabel("[b,b:0.3]"));
    assertEquals("c", TheMapper.findlabel("[b,b:0.3|c,c:0.4]"));
    assertEquals("d", TheMapper.findlabel("[b,c:0.2|c,d:0.5]"));
  }
}
