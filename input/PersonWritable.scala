package org.apache.spark.input


import java.io.{DataInput, DataOutput, IOException}

import org.apache.hadoop.io.{Text, WritableComparable}

import scala.collection.mutable.ArrayBuffer



/**
 *	Coordinate struct
 */
case class Coordinate(x: Double, y: Double, z: Double) {
	def write(out: DataOutput): Unit = {
		out.writeDouble(x)
		out.writeDouble(y)
		out.writeDouble(z)
	}

	override def toString(): String = {
		"($x, $y, $z)"
	}
}

class CoordinateTuple {
	var DT1_TIMESTAMP: Long = _
	var DT1_SB: Coordinate = _
	var DT1_SM: Coordinate = _
	var DT1_NK: Coordinate = _
	var DT1_HD: Coordinate = _
	var DT1_SL: Coordinate = _
	var DT1_EL: Coordinate = _
	var DT1_WL: Coordinate = _
	var DT1_HL: Coordinate = _
	var DT1_SR: Coordinate = _
	var DT1_ER: Coordinate = _
	var DT1_WR: Coordinate = _
	var DT1_HR: Coordinate = _
	var DT1_HPL: Coordinate = _
	var DT1_KL: Coordinate = _
	var DT1_AL: Coordinate = _
	var DT1_FL: Coordinate = _
	var DT1_HPR: Coordinate = _
	var DT1_KR: Coordinate = _
	var DT1_AR: Coordinate = _
	var DT1_FR: Coordinate = _
	var DT1_SS: Coordinate = _
	var DT1_HTL: Coordinate = _
	var DT1_TL: Coordinate = _
	var DT1_HTR: Coordinate = _
	var DT1_TR: Coordinate = _

	var DT2_TimeStamp: Long = _
	var DT2_SB: Coordinate = _
	var DT2_SM: Coordinate = _
	var DT2_NK: Coordinate = _
	var DT2_HD: Coordinate = _
	var DT2_SL: Coordinate = _
	var DT2_EL: Coordinate = _
	var DT2_WL: Coordinate = _
	var DT2_HL: Coordinate = _
	var DT2_SR: Coordinate = _
	var DT2_ER: Coordinate = _
	var DT2_WR: Coordinate = _
	var DT2_HR: Coordinate = _
	var DT2_HPL: Coordinate = _
	var DT2_KL: Coordinate = _
	var DT2_AL: Coordinate = _
	var DT2_FL: Coordinate = _
	var DT2_HPR: Coordinate = _
	var DT2_KR: Coordinate = _
	var DT2_AR: Coordinate = _
	var DT2_FR: Coordinate = _
	var DT2_SS: Coordinate = _
	var DT2_HTL: Coordinate = _
	var DT2_TL: Coordinate = _
	var DT2_HTR: Coordinate = _
	var DT2_TR: Coordinate = _

	var DT3_TimeStamp: Long = _
	var DT3_SB: Coordinate = _
	var DT3_SM: Coordinate = _
	var DT3_NK: Coordinate = _
	var DT3_HD: Coordinate = _
	var DT3_SL: Coordinate = _
	var DT3_EL: Coordinate = _
	var DT3_WL: Coordinate = _
	var DT3_HL: Coordinate = _
	var DT3_SR: Coordinate = _
	var DT3_ER: Coordinate = _
	var DT3_WR: Coordinate = _
	var DT3_HR: Coordinate = _
	var DT3_HPL: Coordinate = _
	var DT3_KL: Coordinate = _
	var DT3_AL: Coordinate = _
	var DT3_FL: Coordinate = _
	var DT3_HPR: Coordinate = _
	var DT3_KR: Coordinate = _
	var DT3_AR: Coordinate = _
	var DT3_FR: Coordinate = _
	var DT3_SS: Coordinate = _
	var DT3_HTL: Coordinate = _
	var DT3_TL: Coordinate = _
	var DT3_HTR: Coordinate = _
	var DT3_TR: Coordinate = _

	var DT4_TimeStamp: Long = _
	var DT4_SB: Coordinate = _
	var DT4_SM: Coordinate = _
	var DT4_NK: Coordinate = _
	var DT4_HD: Coordinate = _
	var DT4_SL: Coordinate = _
	var DT4_EL: Coordinate = _
	var DT4_WL: Coordinate = _
	var DT4_HL: Coordinate = _
	var DT4_SR: Coordinate = _
	var DT4_ER: Coordinate = _
	var DT4_WR: Coordinate = _
	var DT4_HR: Coordinate = _
	var DT4_HPL: Coordinate = _
	var DT4_KL: Coordinate = _
	var DT4_AL: Coordinate = _
	var DT4_FL: Coordinate = _
	var DT4_HPR: Coordinate = _
	var DT4_KR: Coordinate = _
	var DT4_AR: Coordinate = _
	var DT4_FR: Coordinate = _
	var DT4_SS: Coordinate = _
	var DT4_HTL: Coordinate = _
	var DT4_TL: Coordinate = _
	var DT4_HTR: Coordinate = _
	var DT4_TR: Coordinate = _

	var DT5_TimeStamp: Long = _
	var DT5_SB: Coordinate = _
	var DT5_SM: Coordinate = _
	var DT5_NK: Coordinate = _
	var DT5_HD: Coordinate = _
	var DT5_SL: Coordinate = _
	var DT5_EL: Coordinate = _
	var DT5_WL: Coordinate = _
	var DT5_HL: Coordinate = _
	var DT5_SR: Coordinate = _
	var DT5_ER: Coordinate = _
	var DT5_WR: Coordinate = _
	var DT5_HR: Coordinate = _
	var DT5_HPL: Coordinate = _
	var DT5_KL: Coordinate = _
	var DT5_AL: Coordinate = _
	var DT5_FL: Coordinate = _
	var DT5_HPR: Coordinate = _
	var DT5_KR: Coordinate = _
	var DT5_AR: Coordinate = _
	var DT5_FR: Coordinate = _
	var DT5_SS: Coordinate = _
	var DT5_HTL: Coordinate = _
	var DT5_TL: Coordinate = _
	var DT5_HTR: Coordinate = _
	var DT5_TR: Coordinate = _

	var DT6_TimeStamp: Long = _
	var DT6_SB: Coordinate = _
	var DT6_SM: Coordinate = _
	var DT6_NK: Coordinate = _
	var DT6_HD: Coordinate = _
	var DT6_SL: Coordinate = _
	var DT6_EL: Coordinate = _
	var DT6_WL: Coordinate = _
	var DT6_HL: Coordinate = _
	var DT6_SR: Coordinate = _
	var DT6_ER: Coordinate = _
	var DT6_WR: Coordinate = _
	var DT6_HR: Coordinate = _
	var DT6_HPL: Coordinate = _
	var DT6_KL: Coordinate = _
	var DT6_AL: Coordinate = _
	var DT6_FL: Coordinate = _
	var DT6_HPR: Coordinate = _
	var DT6_KR: Coordinate = _
	var DT6_AR: Coordinate = _
	var DT6_FR: Coordinate = _
	var DT6_SS: Coordinate = _
	var DT6_HTL: Coordinate = _
	var DT6_TL: Coordinate = _
	var DT6_HTR: Coordinate = _
	var DT6_TR: Coordinate = _

	var DT1_SB2: Double = _
	var DT1_SM2: Double = _
	var DT1_NK2: Double = _
	var DT1_HD2: Double = _
	var DT1_SL2: Double = _
	var DT1_EL2: Double = _
	var DT1_WL2: Double = _
	var DT1_HL2: Double = _
	var DT1_SR2: Double = _
	var DT1_ER2: Double = _
	var DT1_WR2: Double = _
	var DT1_HR2: Double = _
	var DT1_HPL2: Double = _
	var DT1_KL2: Double = _
	var DT1_AL2: Double = _
	var DT1_FL2: Double = _
	var DT1_HPR2: Double = _
	var DT1_KR2: Double = _
	var DT1_AR2: Double = _
	var DT1_FR2: Double = _
	var DT1_SS2: Double = _
	var DT1_HTL2: Double = _
	var DT1_TL2: Double = _
	var DT1_HTR2: Double = _
	var DT1_TR2: Double = _
	var DT2_SB2: Double = _
	var DT2_SM2: Double = _
	var DT2_NK2: Double = _
	var DT2_HD2: Double = _
	var DT2_SL2: Double = _
	var DT2_EL2: Double = _
	var DT2_WL2: Double = _
	var DT2_HL2: Double = _
	var DT2_SR2: Double = _
	var DT2_ER2: Double = _
	var DT2_WR2: Double = _
	var DT2_HR2: Double = _
	var DT2_HPL2: Double = _
	var DT2_KL2: Double = _
	var DT2_AL2: Double = _
	var DT2_FL2: Double = _
	var DT2_HPR2: Double = _
	var DT2_KR2: Double = _
	var DT2_AR2: Double = _
	var DT2_FR2: Double = _
	var DT2_SS2: Double = _
	var DT2_HTL2: Double = _
	var DT2_TL2: Double = _
	var DT2_HTR2: Double = _
	var DT2_TR2: Double = _
	var DT3_SB2: Double = _
	var DT3_SM2: Double = _
	var DT3_NK2: Double = _
	var DT3_HD2: Double = _
	var DT3_SL2: Double = _
	var DT3_EL2: Double = _
	var DT3_WL2: Double = _
	var DT3_HL2: Double = _
	var DT3_SR2: Double = _
	var DT3_ER2: Double = _
	var DT3_WR2: Double = _
	var DT3_HR2: Double = _
	var DT3_HPL2: Double = _
	var DT3_KL2: Double = _
	var DT3_AL2: Double = _
	var DT3_FL2: Double = _
	var DT3_HPR2: Double = _
	var DT3_KR2: Double = _
	var DT3_AR2: Double = _
	var DT3_FR2: Double = _
	var DT3_SS2: Double = _
	var DT3_HTL2: Double = _
	var DT3_TL2: Double = _
	var DT3_HTR2: Double = _
	var DT3_TR2: Double = _
	var DT4_SB2: Double = _
	var DT4_SM2: Double = _
	var DT4_NK2: Double = _
	var DT4_HD2: Double = _
	var DT4_SL2: Double = _
	var DT4_EL2: Double = _
	var DT4_WL2: Double = _
	var DT4_HL2: Double = _
	var DT4_SR2: Double = _
	var DT4_ER2: Double = _
	var DT4_WR2: Double = _
	var DT4_HR2: Double = _
	var DT4_HPL2: Double = _
	var DT4_KL2: Double = _
	var DT4_AL2: Double = _
	var DT4_FL2: Double = _
	var DT4_HPR2: Double = _
	var DT4_KR2: Double = _
	var DT4_AR2: Double = _
	var DT4_FR2: Double = _
	var DT4_SS2: Double = _
	var DT4_HTL2: Double = _
	var DT4_TL2: Double = _
	var DT4_HTR2: Double = _
	var DT4_TR2: Double = _
	var DT5_SB2: Double = _
	var DT5_SM2: Double = _
	var DT5_NK2: Double = _
	var DT5_HD2: Double = _
	var DT5_SL2: Double = _
	var DT5_EL2: Double = _
	var DT5_WL2: Double = _
	var DT5_HL2: Double = _
	var DT5_SR2: Double = _
	var DT5_ER2: Double = _
	var DT5_WR2: Double = _
	var DT5_HR2: Double = _
	var DT5_HPL2: Double = _
	var DT5_KL2: Double = _
	var DT5_AL2: Double = _
	var DT5_FL2: Double = _
	var DT5_HPR2: Double = _
	var DT5_KR2: Double = _
	var DT5_AR2: Double = _
	var DT5_FR2: Double = _
	var DT5_SS2: Double = _
	var DT5_HTL2: Double = _
	var DT5_TL2: Double = _
	var DT5_HTR2: Double = _
	var DT5_TR2: Double = _
	var DT6_SB2: Double = _
	var DT6_SM2: Double = _
	var DT6_NK2: Double = _
	var DT6_HD2: Double = _
	var DT6_SL2: Double = _
	var DT6_EL2: Double = _
	var DT6_WL2: Double = _
	var DT6_HL2: Double = _
	var DT6_SR2: Double = _
	var DT6_ER2: Double = _
	var DT6_WR2: Double = _
	var DT6_HR2: Double = _
	var DT6_HPL2: Double = _
	var DT6_KL2: Double = _
	var DT6_AL2: Double = _
	var DT6_FL2: Double = _
	var DT6_HPR2: Double = _
	var DT6_KR2: Double = _
	var DT6_AR2: Double = _
	var DT6_FR2: Double = _
	var DT6_SS2: Double = _
	var DT6_HTL2: Double = _
	var DT6_TL2: Double = _
	var DT6_HTR2: Double = _
	var DT6_TR2: Double = _

	var DT0_TimeStamp: Long = _
	var DT0_VALID: Boolean = _
	var DT0_SB: Coordinate = _
	var DT0_SM: Coordinate = _
	var DT0_NK: Coordinate = _
	var DT0_HD: Coordinate = _
	var DT0_SL: Coordinate = _
	var DT0_EL: Coordinate = _
	var DT0_WL: Coordinate = _
	var DT0_HL: Coordinate = _
	var DT0_SR: Coordinate = _
	var DT0_ER: Coordinate = _
	var DT0_WR: Coordinate = _
	var DT0_HR: Coordinate = _
	var DT0_HPL: Coordinate = _
	var DT0_KL: Coordinate = _
	var DT0_AL: Coordinate = _
	var DT0_FL: Coordinate = _
	var DT0_HPR: Coordinate = _
	var DT0_KR: Coordinate = _
	var DT0_AR: Coordinate = _
	var DT0_FR: Coordinate = _
	var DT0_SS: Coordinate = _
	var DT0_HTL: Coordinate = _
	var DT0_TL: Coordinate = _
	var DT0_HTR: Coordinate = _
	var DT0_TR: Coordinate = _
	var DT0_SB2: Double = _
	var DT0_SM2: Double = _
	var DT0_NK2: Double = _
	var DT0_HD2: Double = _
	var DT0_SL2: Double = _
	var DT0_EL2: Double = _
	var DT0_WL2: Double = _
	var DT0_HL2: Double = _
	var DT0_SR2: Double = _
	var DT0_ER2: Double = _
	var DT0_WR2: Double = _
	var DT0_HR2: Double = _
	var DT0_HPL2: Double = _
	var DT0_KL2: Double = _
	var DT0_AL2: Double = _
	var DT0_FL2: Double = _
	var DT0_HPR2: Double = _
	var DT0_KR2: Double = _
	var DT0_AR2: Double = _
	var DT0_FR2: Double = _
	var DT0_SS2: Double = _
	var DT0_HTL2: Double = _
	var DT0_TL2: Double = _
	var DT0_HTR2: Double = _
	var DT0_TR2: Double = _

	var MT_TimeStamp: Long = _
	var MT_SB: Coordinate = _
	var MT_SM: Coordinate = _
	var MT_NK: Coordinate = _
	var MT_HD: Coordinate = _
	var MT_SL: Coordinate = _
	var MT_EL: Coordinate = _
	var MT_WL: Coordinate = _
	var MT_HL: Coordinate = _
	var MT_SR: Coordinate = _
	var MT_ER: Coordinate = _
	var MT_WR: Coordinate = _
	var MT_HR: Coordinate = _
	var MT_HPL: Coordinate = _
	var MT_KL: Coordinate = _
	var MT_AL: Coordinate = _
	var MT_FL: Coordinate = _
	var MT_HPR: Coordinate = _
	var MT_KR: Coordinate = _
	var MT_AR: Coordinate = _
	var MT_FR: Coordinate = _
	var MT_SS: Coordinate = _
	var MT_HTL: Coordinate = _
	var MT_TL: Coordinate = _
	var MT_HTR: Coordinate = _
	var MT_TR: Coordinate = _
}

/**
 *	Person class
 */
class Person {
	var name: String = _
	var actionType: Int = _
	var tuples = new ArrayBuffer[CoordinateTuple]
}



/**
 *	Writable class for Person struct
 */
class PersonWritable
	extends WritableComparable[PersonWritable] {
	private var p = new Person

	/**
	 *	read fields
	 */
	override def readFields(in: DataInput): Unit = {
		def readCoordinate(in: DataInput): Coordinate = {
			val x = in.readDouble
			val y = in.readDouble
			val z = in.readDouble
			new Coordinate(x, y, z)
		}

		p.name = in.readUTF
		p.actionType = in.readInt

		val numOfTuples = in.readInt
		for (i <- 1 to numOfTuples) {
			var t = new CoordinateTuple

			t.DT1_TIMESTAMP = in.readLong
			t.DT1_SB = readCoordinate(in)
			t.DT1_SM = readCoordinate(in)
			t.DT1_NK = readCoordinate(in)
			t.DT1_HD = readCoordinate(in)
			t.DT1_SL = readCoordinate(in)
			t.DT1_EL = readCoordinate(in)
			t.DT1_WL = readCoordinate(in)
			t.DT1_HL = readCoordinate(in)
			t.DT1_SR = readCoordinate(in)
			t.DT1_ER = readCoordinate(in)
			t.DT1_WR = readCoordinate(in)
			t.DT1_HR = readCoordinate(in)
			t.DT1_HPL = readCoordinate(in)
			t.DT1_KL = readCoordinate(in)
			t.DT1_AL = readCoordinate(in)
			t.DT1_FL = readCoordinate(in)
			t.DT1_HPR = readCoordinate(in)
			t.DT1_KR = readCoordinate(in)
			t.DT1_AR = readCoordinate(in)
			t.DT1_FR = readCoordinate(in)
			t.DT1_SS = readCoordinate(in)
			t.DT1_HTL = readCoordinate(in)
			t.DT1_TL = readCoordinate(in)
			t.DT1_HTR = readCoordinate(in)
			t.DT1_TR = readCoordinate(in)

			t.DT2_TimeStamp = in.readLong
			t.DT2_SB = readCoordinate(in)
			t.DT2_SM = readCoordinate(in)
			t.DT2_NK = readCoordinate(in)
			t.DT2_HD = readCoordinate(in)
			t.DT2_SL = readCoordinate(in)
			t.DT2_EL = readCoordinate(in)
			t.DT2_WL = readCoordinate(in)
			t.DT2_HL = readCoordinate(in)
			t.DT2_SR = readCoordinate(in)
			t.DT2_ER = readCoordinate(in)
			t.DT2_WR = readCoordinate(in)
			t.DT2_HR = readCoordinate(in)
			t.DT2_HPL = readCoordinate(in)
			t.DT2_KL = readCoordinate(in)
			t.DT2_AL = readCoordinate(in)
			t.DT2_FL = readCoordinate(in)
			t.DT2_HPR = readCoordinate(in)
			t.DT2_KR = readCoordinate(in)
			t.DT2_AR = readCoordinate(in)
			t.DT2_FR = readCoordinate(in)
			t.DT2_SS = readCoordinate(in)
			t.DT2_HTL = readCoordinate(in)
			t.DT2_TL = readCoordinate(in)
			t.DT2_HTR = readCoordinate(in)
			t.DT2_TR = readCoordinate(in)

			t.DT3_TimeStamp = in.readLong
			t.DT3_SB = readCoordinate(in)
			t.DT3_SM = readCoordinate(in)
			t.DT3_NK = readCoordinate(in)
			t.DT3_HD = readCoordinate(in)
			t.DT3_SL = readCoordinate(in)
			t.DT3_EL = readCoordinate(in)
			t.DT3_WL = readCoordinate(in)
			t.DT3_HL = readCoordinate(in)
			t.DT3_SR = readCoordinate(in)
			t.DT3_ER = readCoordinate(in)
			t.DT3_WR = readCoordinate(in)
			t.DT3_HR = readCoordinate(in)
			t.DT3_HPL = readCoordinate(in)
			t.DT3_KL = readCoordinate(in)
			t.DT3_AL = readCoordinate(in)
			t.DT3_FL = readCoordinate(in)
			t.DT3_HPR = readCoordinate(in)
			t.DT3_KR = readCoordinate(in)
			t.DT3_AR = readCoordinate(in)
			t.DT3_FR = readCoordinate(in)
			t.DT3_SS = readCoordinate(in)
			t.DT3_HTL = readCoordinate(in)
			t.DT3_TL = readCoordinate(in)
			t.DT3_HTR = readCoordinate(in)
			t.DT3_TR = readCoordinate(in)

			t.DT4_TimeStamp = in.readLong
			t.DT4_SB = readCoordinate(in)
			t.DT4_SM = readCoordinate(in)
			t.DT4_NK = readCoordinate(in)
			t.DT4_HD = readCoordinate(in)
			t.DT4_SL = readCoordinate(in)
			t.DT4_EL = readCoordinate(in)
			t.DT4_WL = readCoordinate(in)
			t.DT4_HL = readCoordinate(in)
			t.DT4_SR = readCoordinate(in)
			t.DT4_ER = readCoordinate(in)
			t.DT4_WR = readCoordinate(in)
			t.DT4_HR = readCoordinate(in)
			t.DT4_HPL = readCoordinate(in)
			t.DT4_KL = readCoordinate(in)
			t.DT4_AL = readCoordinate(in)
			t.DT4_FL = readCoordinate(in)
			t.DT4_HPR = readCoordinate(in)
			t.DT4_KR = readCoordinate(in)
			t.DT4_AR = readCoordinate(in)
			t.DT4_FR = readCoordinate(in)
			t.DT4_SS = readCoordinate(in)
			t.DT4_HTL = readCoordinate(in)
			t.DT4_TL = readCoordinate(in)
			t.DT4_HTR = readCoordinate(in)
			t.DT4_TR = readCoordinate(in)

			t.DT5_TimeStamp = in.readLong
			t.DT5_SB = readCoordinate(in)
			t.DT5_SM = readCoordinate(in)
			t.DT5_NK = readCoordinate(in)
			t.DT5_HD = readCoordinate(in)
			t.DT5_SL = readCoordinate(in)
			t.DT5_EL = readCoordinate(in)
			t.DT5_WL = readCoordinate(in)
			t.DT5_HL = readCoordinate(in)
			t.DT5_SR = readCoordinate(in)
			t.DT5_ER = readCoordinate(in)
			t.DT5_WR = readCoordinate(in)
			t.DT5_HR = readCoordinate(in)
			t.DT5_HPL = readCoordinate(in)
			t.DT5_KL = readCoordinate(in)
			t.DT5_AL = readCoordinate(in)
			t.DT5_FL = readCoordinate(in)
			t.DT5_HPR = readCoordinate(in)
			t.DT5_KR = readCoordinate(in)
			t.DT5_AR = readCoordinate(in)
			t.DT5_FR = readCoordinate(in)
			t.DT5_SS = readCoordinate(in)
			t.DT5_HTL = readCoordinate(in)
			t.DT5_TL = readCoordinate(in)
			t.DT5_HTR = readCoordinate(in)
			t.DT5_TR = readCoordinate(in)

			t.DT6_TimeStamp = in.readLong
			t.DT6_SB = readCoordinate(in)
			t.DT6_SM = readCoordinate(in)
			t.DT6_NK = readCoordinate(in)
			t.DT6_HD = readCoordinate(in)
			t.DT6_SL = readCoordinate(in)
			t.DT6_EL = readCoordinate(in)
			t.DT6_WL = readCoordinate(in)
			t.DT6_HL = readCoordinate(in)
			t.DT6_SR = readCoordinate(in)
			t.DT6_ER = readCoordinate(in)
			t.DT6_WR = readCoordinate(in)
			t.DT6_HR = readCoordinate(in)
			t.DT6_HPL = readCoordinate(in)
			t.DT6_KL = readCoordinate(in)
			t.DT6_AL = readCoordinate(in)
			t.DT6_FL = readCoordinate(in)
			t.DT6_HPR = readCoordinate(in)
			t.DT6_KR = readCoordinate(in)
			t.DT6_AR = readCoordinate(in)
			t.DT6_FR = readCoordinate(in)
			t.DT6_SS = readCoordinate(in)
			t.DT6_HTL = readCoordinate(in)
			t.DT6_TL = readCoordinate(in)
			t.DT6_HTR = readCoordinate(in)
			t.DT6_TR = readCoordinate(in)

			t.DT1_SB2 = in.readDouble
			t.DT1_SM2 = in.readDouble
			t.DT1_NK2 = in.readDouble
			t.DT1_HD2 = in.readDouble
			t.DT1_SL2 = in.readDouble
			t.DT1_EL2 = in.readDouble
			t.DT1_WL2 = in.readDouble
			t.DT1_HL2 = in.readDouble
			t.DT1_SR2 = in.readDouble
			t.DT1_ER2 = in.readDouble
			t.DT1_WR2 = in.readDouble
			t.DT1_HR2 = in.readDouble
			t.DT1_HPL2 = in.readDouble
			t.DT1_KL2 = in.readDouble
			t.DT1_AL2 = in.readDouble
			t.DT1_FL2 = in.readDouble
			t.DT1_HPR2 = in.readDouble
			t.DT1_KR2 = in.readDouble
			t.DT1_AR2 = in.readDouble
			t.DT1_FR2 = in.readDouble
			t.DT1_SS2 = in.readDouble
			t.DT1_HTL2 = in.readDouble
			t.DT1_TL2 = in.readDouble
			t.DT1_HTR2 = in.readDouble
			t.DT1_TR2 = in.readDouble
			t.DT2_SB2 = in.readDouble
			t.DT2_SM2 = in.readDouble
			t.DT2_NK2 = in.readDouble
			t.DT2_HD2 = in.readDouble
			t.DT2_SL2 = in.readDouble
			t.DT2_EL2 = in.readDouble
			t.DT2_WL2 = in.readDouble
			t.DT2_HL2 = in.readDouble
			t.DT2_SR2 = in.readDouble
			t.DT2_ER2 = in.readDouble
			t.DT2_WR2 = in.readDouble
			t.DT2_HR2 = in.readDouble
			t.DT2_HPL2 = in.readDouble
			t.DT2_KL2 = in.readDouble
			t.DT2_AL2 = in.readDouble
			t.DT2_FL2 = in.readDouble
			t.DT2_HPR2 = in.readDouble
			t.DT2_KR2 = in.readDouble
			t.DT2_AR2 = in.readDouble
			t.DT2_FR2 = in.readDouble
			t.DT2_SS2 = in.readDouble
			t.DT2_HTL2 = in.readDouble
			t.DT2_TL2 = in.readDouble
			t.DT2_HTR2 = in.readDouble
			t.DT2_TR2 = in.readDouble
			t.DT3_SB2 = in.readDouble
			t.DT3_SM2 = in.readDouble
			t.DT3_NK2 = in.readDouble
			t.DT3_HD2 = in.readDouble
			t.DT3_SL2 = in.readDouble
			t.DT3_EL2 = in.readDouble
			t.DT3_WL2 = in.readDouble
			t.DT3_HL2 = in.readDouble
			t.DT3_SR2 = in.readDouble
			t.DT3_ER2 = in.readDouble
			t.DT3_WR2 = in.readDouble
			t.DT3_HR2 = in.readDouble
			t.DT3_HPL2 = in.readDouble
			t.DT3_KL2 = in.readDouble
			t.DT3_AL2 = in.readDouble
			t.DT3_FL2 = in.readDouble
			t.DT3_HPR2 = in.readDouble
			t.DT3_KR2 = in.readDouble
			t.DT3_AR2 = in.readDouble
			t.DT3_FR2 = in.readDouble
			t.DT3_SS2 = in.readDouble
			t.DT3_HTL2 = in.readDouble
			t.DT3_TL2 = in.readDouble
			t.DT3_HTR2 = in.readDouble
			t.DT3_TR2 = in.readDouble
			t.DT4_SB2 = in.readDouble
			t.DT4_SM2 = in.readDouble
			t.DT4_NK2 = in.readDouble
			t.DT4_HD2 = in.readDouble
			t.DT4_SL2 = in.readDouble
			t.DT4_EL2 = in.readDouble
			t.DT4_WL2 = in.readDouble
			t.DT4_HL2 = in.readDouble
			t.DT4_SR2 = in.readDouble
			t.DT4_ER2 = in.readDouble
			t.DT4_WR2 = in.readDouble
			t.DT4_HR2 = in.readDouble
			t.DT4_HPL2 = in.readDouble
			t.DT4_KL2 = in.readDouble
			t.DT4_AL2 = in.readDouble
			t.DT4_FL2 = in.readDouble
			t.DT4_HPR2 = in.readDouble
			t.DT4_KR2 = in.readDouble
			t.DT4_AR2 = in.readDouble
			t.DT4_FR2 = in.readDouble
			t.DT4_SS2 = in.readDouble
			t.DT4_HTL2 = in.readDouble
			t.DT4_TL2 = in.readDouble
			t.DT4_HTR2 = in.readDouble
			t.DT4_TR2 = in.readDouble
			t.DT5_SB2 = in.readDouble
			t.DT5_SM2 = in.readDouble
			t.DT5_NK2 = in.readDouble
			t.DT5_HD2 = in.readDouble
			t.DT5_SL2 = in.readDouble
			t.DT5_EL2 = in.readDouble
			t.DT5_WL2 = in.readDouble
			t.DT5_HL2 = in.readDouble
			t.DT5_SR2 = in.readDouble
			t.DT5_ER2 = in.readDouble
			t.DT5_WR2 = in.readDouble
			t.DT5_HR2 = in.readDouble
			t.DT5_HPL2 = in.readDouble
			t.DT5_KL2 = in.readDouble
			t.DT5_AL2 = in.readDouble
			t.DT5_FL2 = in.readDouble
			t.DT5_HPR2 = in.readDouble
			t.DT5_KR2 = in.readDouble
			t.DT5_AR2 = in.readDouble
			t.DT5_FR2 = in.readDouble
			t.DT5_SS2 = in.readDouble
			t.DT5_HTL2 = in.readDouble
			t.DT5_TL2 = in.readDouble
			t.DT5_HTR2 = in.readDouble
			t.DT5_TR2 = in.readDouble
			t.DT6_SB2 = in.readDouble
			t.DT6_SM2 = in.readDouble
			t.DT6_NK2 = in.readDouble
			t.DT6_HD2 = in.readDouble
			t.DT6_SL2 = in.readDouble
			t.DT6_EL2 = in.readDouble
			t.DT6_WL2 = in.readDouble
			t.DT6_HL2 = in.readDouble
			t.DT6_SR2 = in.readDouble
			t.DT6_ER2 = in.readDouble
			t.DT6_WR2 = in.readDouble
			t.DT6_HR2 = in.readDouble
			t.DT6_HPL2 = in.readDouble
			t.DT6_KL2 = in.readDouble
			t.DT6_AL2 = in.readDouble
			t.DT6_FL2 = in.readDouble
			t.DT6_HPR2 = in.readDouble
			t.DT6_KR2 = in.readDouble
			t.DT6_AR2 = in.readDouble
			t.DT6_FR2 = in.readDouble
			t.DT6_SS2 = in.readDouble
			t.DT6_HTL2 = in.readDouble
			t.DT6_TL2 = in.readDouble
			t.DT6_HTR2 = in.readDouble
			t.DT6_TR2 = in.readDouble

			t.DT0_TimeStamp = in.readLong
			val str = in.readUTF
			t.DT0_VALID = if (str == "YES") true else false
			t.DT0_SB = readCoordinate(in)
			t.DT0_SM = readCoordinate(in)
			t.DT0_NK = readCoordinate(in)
			t.DT0_HD = readCoordinate(in)
			t.DT0_SL = readCoordinate(in)
			t.DT0_EL = readCoordinate(in)
			t.DT0_WL = readCoordinate(in)
			t.DT0_HL = readCoordinate(in)
			t.DT0_SR = readCoordinate(in)
			t.DT0_ER = readCoordinate(in)
			t.DT0_WR = readCoordinate(in)
			t.DT0_HR = readCoordinate(in)
			t.DT0_HPL = readCoordinate(in)
			t.DT0_KL = readCoordinate(in)
			t.DT0_AL = readCoordinate(in)
			t.DT0_FL = readCoordinate(in)
			t.DT0_HPR = readCoordinate(in)
			t.DT0_KR = readCoordinate(in)
			t.DT0_AR = readCoordinate(in)
			t.DT0_FR = readCoordinate(in)
			t.DT0_SS = readCoordinate(in)
			t.DT0_HTL = readCoordinate(in)
			t.DT0_TL = readCoordinate(in)
			t.DT0_HTR = readCoordinate(in)
			t.DT0_TR = readCoordinate(in)
			t.DT0_SB2 = in.readDouble
			t.DT0_SM2 = in.readDouble
			t.DT0_NK2 = in.readDouble
			t.DT0_HD2 = in.readDouble
			t.DT0_SL2 = in.readDouble
			t.DT0_EL2 = in.readDouble
			t.DT0_WL2 = in.readDouble
			t.DT0_HL2 = in.readDouble
			t.DT0_SR2 = in.readDouble
			t.DT0_ER2 = in.readDouble
			t.DT0_WR2 = in.readDouble
			t.DT0_HR2 = in.readDouble
			t.DT0_HPL2 = in.readDouble
			t.DT0_KL2 = in.readDouble
			t.DT0_AL2 = in.readDouble
			t.DT0_FL2 = in.readDouble
			t.DT0_HPR2 = in.readDouble
			t.DT0_KR2 = in.readDouble
			t.DT0_AR2 = in.readDouble
			t.DT0_FR2 = in.readDouble
			t.DT0_SS2 = in.readDouble
			t.DT0_HTL2 = in.readDouble
			t.DT0_TL2 = in.readDouble
			t.DT0_HTR2 = in.readDouble
			t.DT0_TR2 = in.readDouble

			t.MT_TimeStamp = in.readLong
			t.MT_SB = readCoordinate(in)
			t.MT_SM = readCoordinate(in)
			t.MT_NK = readCoordinate(in)
			t.MT_HD = readCoordinate(in)
			t.MT_SL = readCoordinate(in)
			t.MT_EL = readCoordinate(in)
			t.MT_WL = readCoordinate(in)
			t.MT_HL = readCoordinate(in)
			t.MT_SR = readCoordinate(in)
			t.MT_ER = readCoordinate(in)
			t.MT_WR = readCoordinate(in)
			t.MT_HR = readCoordinate(in)
			t.MT_HPL = readCoordinate(in)
			t.MT_KL = readCoordinate(in)
			t.MT_AL = readCoordinate(in)
			t.MT_FL = readCoordinate(in)
			t.MT_HPR = readCoordinate(in)
			t.MT_KR = readCoordinate(in)
			t.MT_AR = readCoordinate(in)
			t.MT_FR = readCoordinate(in)
			t.MT_SS = readCoordinate(in)
			t.MT_HTL = readCoordinate(in)
			t.MT_TL = readCoordinate(in)
			t.MT_HTR = readCoordinate(in)
			t.MT_TR = readCoordinate(in)

			p.tuples :+= t
		}
	}

	override def write(out: DataOutput): Unit = {
		def writeCoordinateTuple(tu: CoordinateTuple, out: DataOutput): Unit = {
			out.writeLong(tu.DT1_TIMESTAMP)
			tu.DT1_SB.write(out)
			tu.DT1_SM.write(out)
			tu.DT1_NK.write(out)
			tu.DT1_HD.write(out)
			tu.DT1_SL.write(out)
			tu.DT1_EL.write(out)
			tu.DT1_WL.write(out)
			tu.DT1_HL.write(out)
			tu.DT1_SR.write(out)
			tu.DT1_ER.write(out)
			tu.DT1_WR.write(out)
			tu.DT1_HR.write(out)
			tu.DT1_HPL.write(out)
			tu.DT1_KL.write(out)
			tu.DT1_AL.write(out)
			tu.DT1_FL.write(out)
			tu.DT1_HPR.write(out)
			tu.DT1_KR.write(out)
			tu.DT1_AR.write(out)
			tu.DT1_FR.write(out)
			tu.DT1_SS.write(out)
			tu.DT1_HTL.write(out)
			tu.DT1_TL.write(out)
			tu.DT1_HTR.write(out)
			tu.DT1_TR.write(out)

			out.writeLong(tu.DT2_TimeStamp)
			tu.DT2_SB.write(out)
			tu.DT2_SM.write(out)
			tu.DT2_NK.write(out)
			tu.DT2_HD.write(out)
			tu.DT2_SL.write(out)
			tu.DT2_EL.write(out)
			tu.DT2_WL.write(out)
			tu.DT2_HL.write(out)
			tu.DT2_SR.write(out)
			tu.DT2_ER.write(out)
			tu.DT2_WR.write(out)
			tu.DT2_HR.write(out)
			tu.DT2_HPL.write(out)
			tu.DT2_KL.write(out)
			tu.DT2_AL.write(out)
			tu.DT2_FL.write(out)
			tu.DT2_HPR.write(out)
			tu.DT2_KR.write(out)
			tu.DT2_AR.write(out)
			tu.DT2_FR.write(out)
			tu.DT2_SS.write(out)
			tu.DT2_HTL.write(out)
			tu.DT2_TL.write(out)
			tu.DT2_HTR.write(out)
			tu.DT2_TR.write(out)

			out.writeLong(tu.DT3_TimeStamp)
			tu.DT3_SB.write(out)
			tu.DT3_SM.write(out)
			tu.DT3_NK.write(out)
			tu.DT3_HD.write(out)
			tu.DT3_SL.write(out)
			tu.DT3_EL.write(out)
			tu.DT3_WL.write(out)
			tu.DT3_HL.write(out)
			tu.DT3_SR.write(out)
			tu.DT3_ER.write(out)
			tu.DT3_WR.write(out)
			tu.DT3_HR.write(out)
			tu.DT3_HPL.write(out)
			tu.DT3_KL.write(out)
			tu.DT3_AL.write(out)
			tu.DT3_FL.write(out)
			tu.DT3_HPR.write(out)
			tu.DT3_KR.write(out)
			tu.DT3_AR.write(out)
			tu.DT3_FR.write(out)
			tu.DT3_SS.write(out)
			tu.DT3_HTL.write(out)
			tu.DT3_TL.write(out)
			tu.DT3_HTR.write(out)
			tu.DT3_TR.write(out)

			out.writeLong(tu.DT4_TimeStamp)
			tu.DT4_SB.write(out)
			tu.DT4_SM.write(out)
			tu.DT4_NK.write(out)
			tu.DT4_HD.write(out)
			tu.DT4_SL.write(out)
			tu.DT4_EL.write(out)
			tu.DT4_WL.write(out)
			tu.DT4_HL.write(out)
			tu.DT4_SR.write(out)
			tu.DT4_ER.write(out)
			tu.DT4_WR.write(out)
			tu.DT4_HR.write(out)
			tu.DT4_HPL.write(out)
			tu.DT4_KL.write(out)
			tu.DT4_AL.write(out)
			tu.DT4_FL.write(out)
			tu.DT4_HPR.write(out)
			tu.DT4_KR.write(out)
			tu.DT4_AR.write(out)
			tu.DT4_FR.write(out)
			tu.DT4_SS.write(out)
			tu.DT4_HTL.write(out)
			tu.DT4_TL.write(out)
			tu.DT4_HTR.write(out)
			tu.DT4_TR.write(out)

			out.writeLong(tu.DT5_TimeStamp)
			tu.DT5_SB.write(out)
			tu.DT5_SM.write(out)
			tu.DT5_NK.write(out)
			tu.DT5_HD.write(out)
			tu.DT5_SL.write(out)
			tu.DT5_EL.write(out)
			tu.DT5_WL.write(out)
			tu.DT5_HL.write(out)
			tu.DT5_SR.write(out)
			tu.DT5_ER.write(out)
			tu.DT5_WR.write(out)
			tu.DT5_HR.write(out)
			tu.DT5_HPL.write(out)
			tu.DT5_KL.write(out)
			tu.DT5_AL.write(out)
			tu.DT5_FL.write(out)
			tu.DT5_HPR.write(out)
			tu.DT5_KR.write(out)
			tu.DT5_AR.write(out)
			tu.DT5_FR.write(out)
			tu.DT5_SS.write(out)
			tu.DT5_HTL.write(out)
			tu.DT5_TL.write(out)
			tu.DT5_HTR.write(out)
			tu.DT5_TR.write(out)

			out.writeLong(tu.DT6_TimeStamp)
			tu.DT6_SB.write(out)
			tu.DT6_SM.write(out)
			tu.DT6_NK.write(out)
			tu.DT6_HD.write(out)
			tu.DT6_SL.write(out)
			tu.DT6_EL.write(out)
			tu.DT6_WL.write(out)
			tu.DT6_HL.write(out)
			tu.DT6_SR.write(out)
			tu.DT6_ER.write(out)
			tu.DT6_WR.write(out)
			tu.DT6_HR.write(out)
			tu.DT6_HPL.write(out)
			tu.DT6_KL.write(out)
			tu.DT6_AL.write(out)
			tu.DT6_FL.write(out)
			tu.DT6_HPR.write(out)
			tu.DT6_KR.write(out)
			tu.DT6_AR.write(out)
			tu.DT6_FR.write(out)
			tu.DT6_SS.write(out)
			tu.DT6_HTL.write(out)
			tu.DT6_TL.write(out)
			tu.DT6_HTR.write(out)
			tu.DT6_TR.write(out)

			out.writeDouble(tu.DT1_SB2)
			out.writeDouble(tu.DT1_SM2)
			out.writeDouble(tu.DT1_NK2)
			out.writeDouble(tu.DT1_HD2)
			out.writeDouble(tu.DT1_SL2)
			out.writeDouble(tu.DT1_EL2)
			out.writeDouble(tu.DT1_WL2)
			out.writeDouble(tu.DT1_HL2)
			out.writeDouble(tu.DT1_SR2)
			out.writeDouble(tu.DT1_ER2)
			out.writeDouble(tu.DT1_WR2)
			out.writeDouble(tu.DT1_HR2)
			out.writeDouble(tu.DT1_HPL2)
			out.writeDouble(tu.DT1_KL2)
			out.writeDouble(tu.DT1_AL2)
			out.writeDouble(tu.DT1_FL2)
			out.writeDouble(tu.DT1_HPR2)
			out.writeDouble(tu.DT1_KR2)
			out.writeDouble(tu.DT1_AR2)
			out.writeDouble(tu.DT1_FR2)
			out.writeDouble(tu.DT1_SS2)
			out.writeDouble(tu.DT1_HTL2)
			out.writeDouble(tu.DT1_TL2)
			out.writeDouble(tu.DT1_HTR2)
			out.writeDouble(tu.DT1_TR2)
			out.writeDouble(tu.DT2_SB2)
			out.writeDouble(tu.DT2_SM2)
			out.writeDouble(tu.DT2_NK2)
			out.writeDouble(tu.DT2_HD2)
			out.writeDouble(tu.DT2_SL2)
			out.writeDouble(tu.DT2_EL2)
			out.writeDouble(tu.DT2_WL2)
			out.writeDouble(tu.DT2_HL2)
			out.writeDouble(tu.DT2_SR2)
			out.writeDouble(tu.DT2_ER2)
			out.writeDouble(tu.DT2_WR2)
			out.writeDouble(tu.DT2_HR2)
			out.writeDouble(tu.DT2_HPL2)
			out.writeDouble(tu.DT2_KL2)
			out.writeDouble(tu.DT2_AL2)
			out.writeDouble(tu.DT2_FL2)
			out.writeDouble(tu.DT2_HPR2)
			out.writeDouble(tu.DT2_KR2)
			out.writeDouble(tu.DT2_AR2)
			out.writeDouble(tu.DT2_FR2)
			out.writeDouble(tu.DT2_SS2)
			out.writeDouble(tu.DT2_HTL2)
			out.writeDouble(tu.DT2_TL2)
			out.writeDouble(tu.DT2_HTR2)
			out.writeDouble(tu.DT2_TR2)
			out.writeDouble(tu.DT3_SB2)
			out.writeDouble(tu.DT3_SM2)
			out.writeDouble(tu.DT3_NK2)
			out.writeDouble(tu.DT3_HD2)
			out.writeDouble(tu.DT3_SL2)
			out.writeDouble(tu.DT3_EL2)
			out.writeDouble(tu.DT3_WL2)
			out.writeDouble(tu.DT3_HL2)
			out.writeDouble(tu.DT3_SR2)
			out.writeDouble(tu.DT3_ER2)
			out.writeDouble(tu.DT3_WR2)
			out.writeDouble(tu.DT3_HR2)
			out.writeDouble(tu.DT3_HPL2)
			out.writeDouble(tu.DT3_KL2)
			out.writeDouble(tu.DT3_AL2)
			out.writeDouble(tu.DT3_FL2)
			out.writeDouble(tu.DT3_HPR2)
			out.writeDouble(tu.DT3_KR2)
			out.writeDouble(tu.DT3_AR2)
			out.writeDouble(tu.DT3_FR2)
			out.writeDouble(tu.DT3_SS2)
			out.writeDouble(tu.DT3_HTL2)
			out.writeDouble(tu.DT3_TL2)
			out.writeDouble(tu.DT3_HTR2)
			out.writeDouble(tu.DT3_TR2)
			out.writeDouble(tu.DT4_SB2)
			out.writeDouble(tu.DT4_SM2)
			out.writeDouble(tu.DT4_NK2)
			out.writeDouble(tu.DT4_HD2)
			out.writeDouble(tu.DT4_SL2)
			out.writeDouble(tu.DT4_EL2)
			out.writeDouble(tu.DT4_WL2)
			out.writeDouble(tu.DT4_HL2)
			out.writeDouble(tu.DT4_SR2)
			out.writeDouble(tu.DT4_ER2)
			out.writeDouble(tu.DT4_WR2)
			out.writeDouble(tu.DT4_HR2)
			out.writeDouble(tu.DT4_HPL2)
			out.writeDouble(tu.DT4_KL2)
			out.writeDouble(tu.DT4_AL2)
			out.writeDouble(tu.DT4_FL2)
			out.writeDouble(tu.DT4_HPR2)
			out.writeDouble(tu.DT4_KR2)
			out.writeDouble(tu.DT4_AR2)
			out.writeDouble(tu.DT4_FR2)
			out.writeDouble(tu.DT4_SS2)
			out.writeDouble(tu.DT4_HTL2)
			out.writeDouble(tu.DT4_TL2)
			out.writeDouble(tu.DT4_HTR2)
			out.writeDouble(tu.DT4_TR2)
			out.writeDouble(tu.DT5_SB2)
			out.writeDouble(tu.DT5_SM2)
			out.writeDouble(tu.DT5_NK2)
			out.writeDouble(tu.DT5_HD2)
			out.writeDouble(tu.DT5_SL2)
			out.writeDouble(tu.DT5_EL2)
			out.writeDouble(tu.DT5_WL2)
			out.writeDouble(tu.DT5_HL2)
			out.writeDouble(tu.DT5_SR2)
			out.writeDouble(tu.DT5_ER2)
			out.writeDouble(tu.DT5_WR2)
			out.writeDouble(tu.DT5_HR2)
			out.writeDouble(tu.DT5_HPL2)
			out.writeDouble(tu.DT5_KL2)
			out.writeDouble(tu.DT5_AL2)
			out.writeDouble(tu.DT5_FL2)
			out.writeDouble(tu.DT5_HPR2)
			out.writeDouble(tu.DT5_KR2)
			out.writeDouble(tu.DT5_AR2)
			out.writeDouble(tu.DT5_FR2)
			out.writeDouble(tu.DT5_SS2)
			out.writeDouble(tu.DT5_HTL2)
			out.writeDouble(tu.DT5_TL2)
			out.writeDouble(tu.DT5_HTR2)
			out.writeDouble(tu.DT5_TR2)
			out.writeDouble(tu.DT6_SB2)
			out.writeDouble(tu.DT6_SM2)
			out.writeDouble(tu.DT6_NK2)
			out.writeDouble(tu.DT6_HD2)
			out.writeDouble(tu.DT6_SL2)
			out.writeDouble(tu.DT6_EL2)
			out.writeDouble(tu.DT6_WL2)
			out.writeDouble(tu.DT6_HL2)
			out.writeDouble(tu.DT6_SR2)
			out.writeDouble(tu.DT6_ER2)
			out.writeDouble(tu.DT6_WR2)
			out.writeDouble(tu.DT6_HR2)
			out.writeDouble(tu.DT6_HPL2)
			out.writeDouble(tu.DT6_KL2)
			out.writeDouble(tu.DT6_AL2)
			out.writeDouble(tu.DT6_FL2)
			out.writeDouble(tu.DT6_HPR2)
			out.writeDouble(tu.DT6_KR2)
			out.writeDouble(tu.DT6_AR2)
			out.writeDouble(tu.DT6_FR2)
			out.writeDouble(tu.DT6_SS2)
			out.writeDouble(tu.DT6_HTL2)
			out.writeDouble(tu.DT6_TL2)
			out.writeDouble(tu.DT6_HTR2)
			out.writeDouble(tu.DT6_TR2)

			out.writeLong(tu.DT0_TimeStamp)
			out.writeUTF(if (tu.DT0_VALID) "YES" else "NO")
			tu.DT0_SB.write(out)
			tu.DT0_SM.write(out)
			tu.DT0_NK.write(out)
			tu.DT0_HD.write(out)
			tu.DT0_SL.write(out)
			tu.DT0_EL.write(out)
			tu.DT0_WL.write(out)
			tu.DT0_HL.write(out)
			tu.DT0_SR.write(out)
			tu.DT0_ER.write(out)
			tu.DT0_WR.write(out)
			tu.DT0_HR.write(out)
			tu.DT0_HPL.write(out)
			tu.DT0_KL.write(out)
			tu.DT0_AL.write(out)
			tu.DT0_FL.write(out)
			tu.DT0_HPR.write(out)
			tu.DT0_KR.write(out)
			tu.DT0_AR.write(out)
			tu.DT0_FR.write(out)
			tu.DT0_SS.write(out)
			tu.DT0_HTL.write(out)
			tu.DT0_TL.write(out)
			tu.DT0_HTR.write(out)
			tu.DT0_TR.write(out)
			out.writeDouble(tu.DT0_SB2)
			out.writeDouble(tu.DT0_SM2)
			out.writeDouble(tu.DT0_NK2)
			out.writeDouble(tu.DT0_HD2)
			out.writeDouble(tu.DT0_SL2)
			out.writeDouble(tu.DT0_EL2)
			out.writeDouble(tu.DT0_WL2)
			out.writeDouble(tu.DT0_HL2)
			out.writeDouble(tu.DT0_SR2)
			out.writeDouble(tu.DT0_ER2)
			out.writeDouble(tu.DT0_WR2)
			out.writeDouble(tu.DT0_HR2)
			out.writeDouble(tu.DT0_HPL2)
			out.writeDouble(tu.DT0_KL2)
			out.writeDouble(tu.DT0_AL2)
			out.writeDouble(tu.DT0_FL2)
			out.writeDouble(tu.DT0_HPR2)
			out.writeDouble(tu.DT0_KR2)
			out.writeDouble(tu.DT0_AR2)
			out.writeDouble(tu.DT0_FR2)
			out.writeDouble(tu.DT0_SS2)
			out.writeDouble(tu.DT0_HTL2)
			out.writeDouble(tu.DT0_TL2)
			out.writeDouble(tu.DT0_HTR2)
			out.writeDouble(tu.DT0_TR2)

			out.writeLong(tu.MT_TimeStamp)
			tu.MT_SB.write(out)
			tu.MT_SM.write(out)
			tu.MT_NK.write(out)
			tu.MT_HD.write(out)
			tu.MT_SL.write(out)
			tu.MT_EL.write(out)
			tu.MT_WL.write(out)
			tu.MT_HL.write(out)
			tu.MT_SR.write(out)
			tu.MT_ER.write(out)
			tu.MT_WR.write(out)
			tu.MT_HR.write(out)
			tu.MT_HPL.write(out)
			tu.MT_KL.write(out)
			tu.MT_AL.write(out)
			tu.MT_FL.write(out)
			tu.MT_HPR.write(out)
			tu.MT_KR.write(out)
			tu.MT_AR.write(out)
			tu.MT_FR.write(out)
			tu.MT_SS.write(out)
			tu.MT_HTL.write(out)
			tu.MT_TL.write(out)
			tu.MT_HTR.write(out)
			tu.MT_TR.write(out)
		}

		out.writeUTF(p.name)
		out.writeInt(p.actionType)
		out.writeInt(p.tuples.size)
		p.tuples.foreach(tu => {
			writeCoordinateTuple(tu, out)
		})
	}

	override def compareTo(other: PersonWritable): Int = {
		val thisKey = p.name + p.actionType
		val otherKey = other.p.name + other.p.actionType

		thisKey.compareTo(otherKey)
	}

	override def toString(): String = {
		val thisKey = p.name + p.actionType
		s"key: ${thisKey}, # of tuples: ${p.tuples.size}"
	}
}