// package org.apache.spark.input

// import org.apache.hadoop.mapreduce.{InputSplit, TaskAttemptContext}
// import org.apache.hadoop.mapreduce.lib.input.{FileSplit, FileInputFormat}


// /**
//  *	Reads records that are divided by a specific format
//  */
// class PersonInputFormat
// 	extends FileInputFormat[LongWritable, PersonWritable] {
// 	override def createRecordReader(split: InputSplit, context: TaskAttemptContext) = {
// 		new PersonRecordReader(split.value.asInstanceOf[FileSplit], context.getConfiguration)
// 	}
// }