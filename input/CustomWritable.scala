package org.apache.spark.input


import java.io.{DataInput, DataOutput, IOException, ObjectInput, ObjectOutput, Serializable}
// import java.util.ArrayList

import org.apache.hadoop.io.{Text, WritableComparable}
import scala.collection.mutable.ArrayBuffer


/**
 *	Person class
 */
class Person_kjh {
	var name: String = _
	var actionType: Long = _
	var timeStamp: Long = _
	var coords = new ArrayBuffer[Double]
}

/**
 *	Writable class for Person struct
 */
class PersonWritable_kjh
	extends WritableComparable[PersonWritable_kjh] {
	
	private var p = new Person_kjh

	def getP: Person_kjh = {
		p
	}

	/**
	 *	override methods
	 */
	override def readFields(in: DataInput): Unit = {
		p.name = in.readUTF
		p.actionType = in.readLong
		p.timeStamp = in.readLong

		val numOfCoords = in.readLong
		for (i <- 1 to numOfCoords) {
			p.coords += in.readDouble
		}
	}

	override def write(out: DataOutput): Unit = {
		// println("***def write : " + p.name)
		// println("***def write : " + p.actionType)
		// println("***def write : " + p.timeStamp)

		out.writeUTF(p.name)
		out.writeLong(p.actionType)
		out.writeLong(p.timeStamp)

		out.writeLong(p.coords.size)
		p.coords.foreach(coord => {
			out.writeDouble(coord)
		})
	}

	override def compareTo(other: PersonWritable_kjh): Int = {
		val thisKey = p.name + p.actionType
		val otherKey = other.p.name + other.p.actionType

		thisKey.compareTo(otherKey)
	}

	override def toString(): String = {
		val thisKey = p.name + p.actionType
		s"key: ${thisKey}, # of tuples: "
	}
}