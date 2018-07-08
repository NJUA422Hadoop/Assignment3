package mission1.output;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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
  private Configuration conf;
  private FileSystem fileSystem;

  private Map<String, FSDataOutputStream> fileStreams = new HashMap<>();

  private final Logger logger = Logger.getLogger(TheRecordWriter.class);

  public TheRecordWriter(TaskAttemptContext job) {
    this.job = job;
    this.conf = this.job.getConfiguration();

    try {
      fileSystem = FileSystem.newInstance(this.conf);
    } catch(IOException ioe) {
      logger.error(ioe);
    }
  }

  @Override
  public void write(TheKey key, Text value) throws IOException, InterruptedException {
    String name = key.first;

    FSDataOutputStream fileStream = fileStreams.get(name);

    if (fileStream == null) {
      fileStream = fileSystem.create(new Path(conf.get("output") + "/" + name));

      fileStreams.put(name, fileStream);
    }

    fileStream.write(value.getBytes());
    fileStream.write('\n');
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    for (FSDataOutputStream u : fileStreams.values()) {
      u.close();
    }
  }
}