package org.apache.spark.input


import scala.collection.JavaConversions._
import scala.util.control.Breaks._

import java.io.{DataInput, DataOutput, IOException}
import java.util.ArrayList

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.hadoop.mapred.{InputSplit, FileInputFormat,
	FileSplit, JobConf, LineRecordReader, RecordReader, Reporter}
import org.apache.hadoop.util.LineReader


class CSHPersonInputFormat
	extends FileInputFormat[LongWritable, PersonWritable] {
	override def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter)
		: RecordReader[LongWritable, PersonWritable] = {
		reporter.setStatus(split.toString)
		new CSHPersonRecordReader(job, split.asInstanceOf[FileSplit])
	}

	override def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
		def getPersonSplits(status: FileStatus, conf: Configuration): List[FileSplit] = {
			val splits = new ArrayList[FileSplit]()

			val fileName = status.getPath
			if (status.isDirectory) {
				throw new IOException("Not a file: " + fileName)
			}


			var fs = fileName.getFileSystem(conf)
			var lr: LineReader = null
			var line = new Text()

			var begin: Long = 0
			var length: Long = 0

			var numOfTuple: Int = 0	//length of person record
			var bytes: Int = -1

			breakable {
				while (true) {
					length = 0

					//make one record to FileSplit
					bytes = lr.readLine(line)	//name,type
					if (bytes == 0) {	//bytes 반환값 --> byte 단위
						break
					}
					length += bytes


					bytes = lr.readLine(line)
					length += bytes
					numOfTuple = line.toString.toInt	//# of tuples


					// TODO: 어떻게 레코드들 한번에 skip 할 수 있는지?
					for (i <- 1 to numOfTuple) {
						bytes = lr.readLine(line)
						length += bytes
					}

					//splits :+= createFileSplit(fileName, begin, length)
					splits.add(createFileSplit(fileName, begin, length))

					begin += length
					length = 0
				}
			}


			lr.close

			splits.toList
		}	//getPersonSplits


		val splits = new ArrayList[FileSplit]()
		listStatus(job).foreach(getPersonSplits(_, job)
			.foreach {
			//_ => splits :+= split.asInstanceOf[FileSplit]
			split => splits.add(split)
		})

		splits.toArray.asInstanceOf[Array[InputSplit]]
	}

	def createFileSplit(fileName: Path, begin: Long, length: Long): FileSplit = {
		if (begin == 0)
			new FileSplit(fileName, begin, length-1, Array[String]())
		else
			new FileSplit(fileName, begin-1, length, Array[String]())
	}
}

class CSHPersonRecordReader(job: JobConf, split: FileSplit)
	extends RecordReader[LongWritable, PersonWritable] {

		//TODO
		// RecordReader 어떻게 구현해야 하는지

	private var lineReader = new LineRecordReader(job, split)
	private var lineKey = lineReader.createKey
	private var lineValue = lineReader.createValue


	/**
	 *	override
	 */
	override def next(key: LongWritable, value: PersonWritable): Boolean = {
		false
	}

	override def createKey: LongWritable = {
		new LongWritable
	}

	override def createValue: PersonWritable = {
		new PersonWritable
	}

	override def getPos: Long = {
		lineReader.getPos
	}

	override def close: Unit = {
		lineReader.close
	}

	override def getProgress: Float = {
		lineReader.getProgress
	}
}