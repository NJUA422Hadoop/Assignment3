package partitioner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class InvertedIndexPartitionerTest {
    private InvertedIndexPartitioner instance;

    @BeforeEach
    void setup() {
        instance = new InvertedIndexPartitioner();
    }

    @AfterEach
    void cleanup() {
    }

    @Test
    @DisplayName("对key的字符串分割测试")
    public void getWordTest() {
        String test = "test_word";
        // 确保@和#都能正确分割字符串
        assertEquals(instance.getWord(new Text(test + "@file_name")), new Text(test));
        assertEquals(instance.getWord(new Text(test + "#file_name")), new Text(test));
    }
}