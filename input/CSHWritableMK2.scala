/**
 *	2017/03/01
 *	읽을 때는 PersonRecord 단위로 읽고
 *	이후 Reduce를 통해 이를 CSHWritableMK2로 변환
 */

package org.apache.spark.input

import java.io.{DataInput, DataOutput}


/**
 *	기본 읽는 단위.
 */
class PersonRecord {
	var name: String = _
	var actionType: Long = _
	var timestamp: Long = _

	//coords
	var coordinates = new ArrayBuffer[Double]
}

/**
 *	Writable wrapper for Person records
 *	reduce (Key, Value) = (이름+ActionType, CSHWritableMK2)
 */
class CSHWritableMK2
	extends WritableComparable[CSHWritableMK2] {

	/**
	 *	override
	 */
	override def readFields(in: DataInput): Unit = {

	}

	override def write(out: DataOutput): Unit = {

	}

	override def compareTo(other: CSHWritableMK2): Int = {
		0
	}

	override def toString(): String = {
		s""
	}
}

/**
 *	Person Record Reader
 */
class CSHPersonRecordReader
	extends RecordReader[String, ???] {
}

/**
 *	sc.newAPIHadoopFile 등에 넣을 InputFormat
 */
class CSHPersonRecordInputFormat
	extends FileInputFormat[String, ???] {
	/**
	 *	override
	 */
	override def createRecordReader(arg0: InputSplit,
		arg1: TaskAttemptContext) = {
		new CSHPersonRecordReader
	}
}