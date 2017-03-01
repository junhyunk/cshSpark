package org.apache.spark.input

import java.io.{DataInput, DataOutput}

import org.apache.hadoop.io.{Text, WritableComparable}
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.{InputSplit, FileSplit, JobConf, LineRecordReader,
	RecordReader, Reporter}


/**
 *	Tutorial
 *
 *	https://developer.yahoo.com/hadoop/tutorial/module5.html
 */
class PointWritable
	extends WritableComparable[PointWritable] {
	var x: Float = _
	var y: Float = _
	var z: Float = _


	/**
	 *	override
	 */
	override def write(out: DataOutput) = {
		out.writeFloat(x)
		out.writeFloat(y)
		out.writeFloat(z)
	}

	override def readFields(in: DataInput) = {
		x = in.readFloat
		y = in.readFloat
		z = in.readFloat
	}

	override def toString: String = {
		s"$x, $y, $z"
	}

	override def compareTo(other: PointWritable): Int  = {
		def distFromOrig(x: Float, y: Float, z: Float): Float
			= Math.sqrt(x*x + y*y + z*z).toFloat

		val myDist = distFromOrig(x, y, z)
		val otherDist = distFromOrig(other.x, other.y, other.z)

		myDist compareTo otherDist
	}

	override def equals(other: Any): Boolean = {
		other match {
			case other: PointWritable => this.hashCode == other.hashCode
			case _ => false
		}
	}

	override def hashCode(): Int = {
		java.lang.Float.floatToIntBits(x) ^ java.lang.Float.floatToIntBits(y) ^ java.lang.Float.floatToIntBits(z)
	}
}

class PointInputFormat
	extends FileInputFormat[Text, PointWritable] {
	def getRecordReader(split: InputSplit, job: JobConf, reporter: Reporter)
		: RecordReader[Text, PointWritable] = {
		reporter.setStatus(split.toString)
		new PointRecordReader(job, split.asInstanceOf[FileSplit])
	}
}

class PointRecordReader(job: JobConf, split: FileSplit)
	extends RecordReader[Text, PointWritable] {
	private var lineReader = new LineRecordReader(job, split)
	private var lineKey = lineReader.createKey
	private var lineValue = lineReader.createValue


	/**
	 *	override
	 */
	override def next(key: Text, value: PointWritable): Boolean = {
		if (!lineReader.next(lineKey, lineValue))
			false

		val pieces = lineValue.toString.split(',').map(_.trim)
		val (fx, fy, fz) = (pieces(1).toFloat, pieces(2).toFloat,
			pieces(3).toFloat)

		key.set(pieces(0))
		
		value.x = fx
		value.y = fy
		value.z = fz

		true
	}

	override def createKey: Text = {
		new Text
	}

	override def createValue: PointWritable = {
		new PointWritable
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