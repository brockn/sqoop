package org.apache.sqoop.mapreduce;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.MergeMapperBase;



public class MergeParquetMapper
    extends MergeGenericRecordExportMapper<GenericRecord, NullWritable> {

  @Override
  protected void setup(Context c) throws IOException, InterruptedException {
    super.setup(c);
  }

  @Override
  protected void map(GenericRecord key, NullWritable val, Context c)
      throws IOException, InterruptedException {
    processRecord(toSqoopRecord(key), c);
  }

}
