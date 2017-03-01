package org.apache.spark.input

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

class MyRecordReader extends RecordReader[PersonWritable_kjh, LongWritable] {
    private var key: PersonWritable_kjh = null
    private var value: LongWritable = null
    private var reader = new LineRecordReader

    override def close(): Unit = {
        reader.close
    }

	override def getCurrentKey(): PersonWritable_kjh = {
		key
	}
    
    override def getCurrentValue(): LongWritable = {
		value
	}

	override def getProgress(): Float = {
		reader.getProgress
	}

    override def initialize(is: InputSplit, tac: TaskAttemptContext): Unit = {
        reader.initialize(is, tac)
    }

    override def nextKeyValue(): Boolean = {
        var gotNextKeyValue = reader.nextKeyValue
        if(gotNextKeyValue){
            if(key == null) {
                key = new PersonWritable_kjh
            }
            if(value == null) {
                value = new LongWritable
            }
            
            var line = reader.getCurrentValue   // Text
            var tokens = line.toString.split(",").map(_.trim)
            key.getP.name = tokens(0)
            key.getP.actionType = tokens(1).toLong
            key.getP.timeStamp = tokens(2).toLong
            /**var i = 3
            for(i <- 3 to 4) {
                key.getP.coords.add(tokens(i).toDouble)
            }*/
            value.set(1)
        }
        else {
            key = null
            value = null
        }
        gotNextKeyValue
    }

	//override def createKey: PersonWritable_kjh = {
	//	new PersonWritable_kjh
	//}

	//override def createValue: LongWritable = {
	//	new LongWritable
	//}

	//override def getPos: Long = {
	//	reader.getPos
	//}
	
    //override def next(key: LongWritable, value: PersonWritable): Boolean = {
	//	false
	//}
}

class Person_Input extends FileInputFormat[PersonWritable_kjh, LongWritable] {
    override def createRecordReader(arg0: InputSplit, arg1: TaskAttemptContext)
    : RecordReader[PersonWritable_kjh, LongWritable] = {
        new MyRecordReader
    }
}