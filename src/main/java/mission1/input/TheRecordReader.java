package mission1.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class TheRecordReader extends RecordReader<Text, Text> {
  private Long start;
  private Long end;
  private Long now;

  private FSDataInputStream file;

  private Text key;
  private Text value;

  private LineReader reader;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit) split;

    start = fileSplit.getStart();
    end = start + fileSplit.getLength();
    
    Configuration conf = context.getConfiguration();
    
    Path path = fileSplit.getPath();
    FileSystem fileSystem = path.getFileSystem(conf);
    
    file = fileSystem.open(path);
    file.seek(start);
    
    reader = new LineReader(file);
    
    now = Long.valueOf(start);

    key = new Text();
    value = new Text();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (now > end) {
      return false;
    }

    now += reader.readLine(value);

    if (value.getLength() == 0) {
      return false;
    }

    // TODO

    return true;
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    float pall = end - start;

    if (pall == 0) {
      return 0;
    }

    float pnow = now - start;

    return Math.min(pnow / pall, 1);
  }

  @Override
  public void close() throws IOException {
    file.close();
  }
}