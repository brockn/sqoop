package org.apache.sqoop.mapreduce;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.sqoop.avro.AvroUtil;

import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;

public class MergeParquetReducer extends Reducer<Text, MergeRecord, GenericRecord, NullWritable> {

  private Schema schema = null;
  private boolean bigDecimalFormatString = true;
  private LargeObjectLoader lobLoader = null;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    schema = ParquetJob.getAvroSchema(conf);
    bigDecimalFormatString = conf.getBoolean(ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
        ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
    lobLoader = new LargeObjectLoader(conf, new Path(
        conf.get("sqoop.kite.lob.extern.dir", "/tmp/sqoop-parquet-" + context.getTaskAttemptID())));
  }


  @Override
  public void reduce(Text key, Iterable<MergeRecord> vals, Context c)
      throws IOException, InterruptedException {
    SqoopRecord bestRecord = null;
    try {
      for (MergeRecord val : vals) {
        if (null == bestRecord && !val.isNewRecord()) {
          // Use an old record if we don't have a new record.
          bestRecord = (SqoopRecord) val.getSqoopRecord().clone();
        } else if (val.isNewRecord()) {
          bestRecord = (SqoopRecord) val.getSqoopRecord().clone();
        }
      }
    } catch (CloneNotSupportedException cnse) {
      throw new IOException(cnse);
    }

    if (null != bestRecord) {
      try {
        // Loading of LOBs was delayed until we have a Context.
        bestRecord.loadLargeObjects(lobLoader);
      } catch (SQLException sqlE) {
        throw new IOException(sqlE);
      }
      GenericRecord outKey =
          AvroUtil.toGenericRecord(bestRecord.getFieldMap(), schema, bigDecimalFormatString);
      c.write(outKey, null);
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException {
    if (null != lobLoader) {
      lobLoader.close();
    }
  }
}
