package org.apache.sqoop.mapreduce;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.sqoop.avro.AvroUtil;

import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.cloudera.sqoop.orm.ClassWriter;

public class MergeGenericRecordExportMapper <K, V>
extends AutoProgressMapper<K, V, Text, MergeRecord> {

public static final String AVRO_COLUMN_TYPES_MAP = "sqoop.avro.column.types.map";

protected MapWritable columnTypes;



private String keyColName; // name of the key column.
private boolean isNew; // true if this split is from the new dataset.
private SqoopRecord recordImpl;

@Override
protected void setup(Context context)
    throws IOException, InterruptedException {
  Configuration conf = context.getConfiguration();
  keyColName = conf.get(MergeJob.MERGE_KEY_COL_KEY);

  InputSplit is = context.getInputSplit();
  FileSplit fs = (FileSplit) is;
  Path splitPath = fs.getPath();

  if (splitPath.toString().startsWith(
      conf.get(MergeJob.MERGE_NEW_PATH_KEY))) {
    this.isNew = true;
  } else if (splitPath.toString().startsWith(
      conf.get(MergeJob.MERGE_OLD_PATH_KEY))) {
    this.isNew = false;
  } else {
    throw new IOException("File " + splitPath + " is not under new path "
        + conf.get(MergeJob.MERGE_NEW_PATH_KEY) + " or old path "
        + conf.get(MergeJob.MERGE_OLD_PATH_KEY));
  }
  
  
  String recordClassName = "GenericRecord";
  
  try {
    Class cls = Class.forName(recordClassName, true,
        Thread.currentThread().getContextClassLoader());
    recordImpl = (SqoopRecord) ReflectionUtils.newInstance(cls, conf);
  } catch (ClassNotFoundException cnfe) {
    throw new IOException(cnfe);
  }

  if (null == recordImpl) {
    throw new IOException("Could not instantiate object of type "
        + recordClassName);
  }
  
}

protected SqoopRecord toSqoopRecord(GenericRecord record) throws IOException {
  Schema avroSchema = record.getSchema();
  for (Map.Entry<Writable, Writable> e : columnTypes.entrySet()) {
    String columnName = e.getKey().toString();
    String columnType = e.getValue().toString();
    String cleanedCol = ClassWriter.toIdentifier(columnName);
    Schema.Field field = getFieldIgnoreCase(avroSchema, cleanedCol);
    if (null == field) {
      throw new IOException("Cannot find field " + cleanedCol
          + " in Avro schema " + avroSchema);
    }

    Object avroObject = record.get(field.name());
    Object fieldVal = AvroUtil.fromAvro(avroObject, field.schema(), columnType);
    recordImpl.setField(cleanedCol, fieldVal);
  }
  return recordImpl;
}

protected void processRecord(SqoopRecord r, Context c)
    throws IOException, InterruptedException {
  MergeRecord mr = new MergeRecord(r, isNew);
  Map<String, Object> fieldMap = r.getFieldMap();
  if (null == fieldMap) {
    throw new IOException("No field map in record " + r);
  }
  Object keyObj = fieldMap.get(keyColName);
  if (null == keyObj) {
    throw new IOException("Cannot join values on null key. "
        + "Did you specify a key column that exists?");
  } else {
    c.write(new Text(keyObj.toString()), mr);
  }
}



private static Schema.Field getFieldIgnoreCase(Schema avroSchema,
  String fieldName) {
for (Schema.Field field : avroSchema.getFields()) {
  if (field.name().equalsIgnoreCase(fieldName)) {
    return field;
  }
}
return null;
}

}