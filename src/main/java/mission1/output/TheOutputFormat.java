package mission1.output;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mission1.tools.TheKey;

public class TheOutputFormat extends FileOutputFormat<TheKey, Text> {
  @Override
  public RecordWriter<TheKey, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    return new TheRecordWriter(job);
  }
}