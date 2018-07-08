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
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

import mission1.tools.TheKey;

public class TheRecordReader extends RecordReader<TheKey, Text> {
  private Long start;
  private Long end;
  private Long pos;
  private Long lineCount;
  private Text line;

  private FSDataInputStream file;

  private TheKey key;
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
    
    pos = Long.valueOf(start);
    lineCount = Long.valueOf(0);
    
    line = new Text();

    key = new TheKey(
      path.getName(),
      Long.valueOf(0)
    );
    value = new Text();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (pos >= end) {
      return false;
    }

    pos += reader.readLine(line);

    lineCount++;

    key.second = lineCount;
    value.set(line);

    return true;
  }

  @Override
  public TheKey getCurrentKey() throws IOException, InterruptedException {
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

    float pnow = pos - start;

    return Math.min(pnow / pall, 1);
  }

  @Override
  public void close() throws IOException {
    file.close();
  }
}