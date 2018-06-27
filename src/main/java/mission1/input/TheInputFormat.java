package mission1.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import mission1.tools.TheKey;

public class TheInputFormat extends FileInputFormat<TheKey, Text> {
  @Override
  public RecordReader<TheKey, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new TheRecordReader();
  }
}