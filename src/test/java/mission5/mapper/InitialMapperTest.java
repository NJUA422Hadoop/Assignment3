package mission5.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.Test;

public class InitialMapperTest {
  @Test
  public void mychangestrTest() {
    assertEquals("[b,b:0.3]", InitialMapper.mychangestr("[b:0.3]"));
    assertEquals("[b,b:0.3|c,c:0.5]", InitialMapper.mychangestr("[b:0.3|c:0.5]"));
  }
}