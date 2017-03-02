/**
 *	2017/03/01
 *	읽을 때는 PersonRecord 단위로 읽고
 *	이후 Reduce를 통해 이를 CSHWritableMK2로 변환
 *
 *	2017/03/02
 *	(이름+type, PersonRecord)
 *	--> Reduce
 *	--> (이름+type, Writable)
 */

package org.apache.spark.input

import java.io.{DataInput, DataOutput}


/**
 *	기본 읽는 단위.
 */
class PersonRecord {
	// var name: String = _
	// var actionType: Long = _
	var timestamp: Long = _

	//coords
	var coordinates = new ArrayBuffer[Double]
}

/**
 *	Writable wrapper for Person records
 *	reduce (Key, Value) = (이름+actionType, CSHWritableMK2)
 */
class CSHWritableMK2
	extends WritableComparable[CSHWritableMK2] {

	private var name = ""
	private var actionType = 0L
	private var record = new PersonRecord

	/**
	 *	override
	 */
	override def readFields(in: DataInput): Unit = {
		name = in.readUTF
		actionType = in.readLong
		record.timestamp = in.readLong

		val numOfCoords = in.readLong
		for (i <- 1 to numOfCoords) {
			record.coordinates += in.readDouble
		}
	}

	override def write(out: DataOutput): Unit = {
		writeUTF(name)
		writeLong(actionType)
		out.writeLong(record.timestamp)

		out.writeInt(record.coordinates.size)
		record.coordinates.foreach(coord => {
			out.writeDouble(coord)
		})
	}

	override def compareTo(other: CSHWritableMK2): Int = {
		0
	}

	override def toString(): String = {
		s"name: ${name}, type: ${actionType}"
	}
}

/**
 *	Person Record Reader
 */
class CSHPersonRecordReader
	extends RecordReader[String, CSHWritableMK2] {

	private var splitStart = 0L
	private var splitEnd = 0L
	private var currentPosition = 0L

	private var fileInputStream: FSDataInputStream = null
	private var recordKey = ""
	private var recordValue = new PersonRecord


	/**
	 *	override
	 */
	override def close(): Unit = {
		if (fileInputStream != null) {
			fileInputStream.close
		}
	}

	override def getCurrentKey() = recordKey

	override def getCurrentValue() = recordValue

	override def getProgress() = {

	}

	override def initialize(inputSplit: InputSplit,
		context: TaskAttemptContext) = {

	}

	override def nextKeyValue(): Boolean = {
		/**
		 *	Update recordKey, recordValue
		 */
		false
	}
}

/**
 *	sc.newAPIHadoopFile 등에 넣을 InputFormat
 */
class CSHPersonRecordInputFormat
	extends FileInputFormat[String, CSHWritableMK2] {
	/**
	 *	override
	 */
	override def createRecordReader(arg0: InputSplit,
		arg1: TaskAttemptContext) = {
		new CSHPersonRecordReader
	}
}