package mission1.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import mission1.tools.TheKey;

public class TheRecordWriter extends RecordWriter<TheKey, Text> {
  private TaskAttemptContext job;
  private FileSystem fileSystem;

  private Map<Text, FSDataOutputStream> fileStreams = new HashMap<>();

  private final Logger logger = Logger.getLogger(TheRecordWriter.class);

  public TheRecordWriter(TaskAttemptContext job) {
    this.job = job;

    try {
      fileSystem = FileSystem.newInstance(this.job.getConfiguration());
    } catch(IOException ioe) {
      logger.error(ioe);
    }
  }

  @Override
  public void write(TheKey key, Text value) throws IOException, InterruptedException {
    Text name = key.first;

    FSDataOutputStream fileStream = fileStreams.get(name);

    if (fileStream == null) {
      fileStream = fileSystem.create(new Path(name.toString()));

      fileStreams.put(name, fileStream);
    }

    fileStream.write(value.getBytes());
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    fileStreams.forEach(new BiConsumer<Text, FSDataOutputStream>() {
      @Override
      public void accept(Text t, FSDataOutputStream u) {
        try {
          u.close();
        } catch(IOException ioe) {
          logger.error(ioe);
        }
      }
    });
  }
}