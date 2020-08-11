package com.flink.demo

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCount {
def main(args: Array[String]): Unit = {

    val map = ReadFile.readFile()
    ReadFile.printContent(map)

  }
}
object ReadFile {
 /*
    This object has two functions
      1. readFile(): read the file line by line and split the string from the spaces
      and put them into Map. Map Key: the word found, Value: occurrences of the word

      2. printContent() : simple print function for print a Map.
  */

  def readFile(): Map[String, Int] ={
    val counter = scala.io.Source.fromFile("sample-csv.csv")
      .getLines
      .flatMap(_.split("\\W+"))
      .foldLeft(Map.empty[String, Int]){
        (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
      }
    return counter
  }

  def printContent(counter : Map[String, Int]): Unit = {
    for ((k,v) <- counter) printf("Claim amount: %s repeated: %d time(s) in 2019\n", k, v)
  }

}