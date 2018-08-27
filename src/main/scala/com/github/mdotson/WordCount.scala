package com.github.mdotson

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

object WordCount {

  def countWords(lines: DataStream[String], stopWords: Set[String], window: Time): DataStream[String] = {
    lines
//      .flatMap(line => line.split(" "))
//      .filter(word => !word.isEmpty)
      .map(word => word.toUpperCase())
//      .filter(word => !stopWords.contains(word))
//      .map(word => (word, 1))
//      .keyBy(0)
//      .timeWindow(window)
//      .sum(1)
  }

}