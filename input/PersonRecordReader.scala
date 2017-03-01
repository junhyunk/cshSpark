// package org.apache.spark.input

// import java.io.IOException

// import com.google.common.io.Closeables

// import org.apache.hadoop.conf.Configuration
// import org.apache.hadoop.io.{DataOutputBuffer, Text}
// import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
// import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
// import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}


// class PersonRecordReader(split: FileSplit, conf: Configuration)
// 	extends RecordReader[LongWritable, PersonWritable] {
	
// }