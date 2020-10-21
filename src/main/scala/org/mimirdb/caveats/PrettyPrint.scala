package org.mimirdb.caveats

import org.apache.spark.sql.{ DataFrame, Row }
import com.typesafe.scalalogging.LazyLogging
import scala.collection.mutable.Buffer
import org.jline.terminal.{Terminal,TerminalBuilder}
import org.jline.utils.{AttributedStyle,AttributedStringBuilder,AttributedString}
import org.mimirdb.caveats.implicits._

object PrettyPrint
  extends LazyLogging
{
  var simpleOutput = false
  lazy val format: OutputFormat = 
  {
    if(simpleOutput) { DefaultOutputFormat }
    else {
      try { 
        new PrettyOutputFormat(
          TerminalBuilder.builder()
                         // .`type`("dumb-color")
                         .system(true)
                         .build()
        )
      } catch {
        case e:Exception => {
          logger.error(s"Error initializing terminal (${e.getMessage()}); falling back to naive")
          DefaultOutputFormat
        }
      }
    }
  }

  def showWithCaveats(df:DataFrame, count: Int = 10)
  {
    format.print(df, count)
  }

  trait OutputFormat
  {
    def print(msg: String): Unit
    def print(df: DataFrame, count: Int): Unit
    def printRaw(msg: Array[Byte]): Unit
  }

  def center(str: String, length: Int): String =
  {
    if(str.size > length){ 
      if(length < 1){ return str.substring(0, 1) }
      else { return str.substring(0, length-1)+"…" }
    } else { 
      val lhs = (length - str.size) / 2
      val rhs = length - (str.size + lhs)
      (" "*lhs) + str + (" "*rhs)
    }
  }

  def formatTable(rows: Seq[Seq[String]]): Seq[Seq[String]] =
  {
    val lengths = 
      rows.map { _.map { _.size } }
          .fold(Seq()) { _.zipAll(_, 0, 0)
                          .map { x => Seq(x._1, x._2).max } } 

    rows.map { row =>
      row.zipAll(lengths, "", 0)
         .map { x => center(x._1, x._2) }
    }
  }

  object DefaultOutputFormat
    extends OutputFormat
  {
    def print(msg: String) 
    {
      println(msg);
    }

    def toStrings(row: Row): (Seq[String], Option[String]) =
    {
      (
        row.caveattedAttributes
           .map { 
            case (field, isCaveatted)  =>
              (
                if(field == null){ "null" }
                else { field.toString }
              ) + (
                if(isCaveatted){ "*" } else { "" }
              )
        }, 
        if(row.isCaveatted){ Some("  <- caveatted row") } else { None }
      )
    }
    def print(df: DataFrame, count: Int)
    {
      val fields: Seq[String] = 
        df.schema.fieldNames.dropRight(1)
      val (outputs, caveatNotes): (Seq[Seq[String]], Seq[Option[String]]) = 
        df.take(count+1).map { toStrings(_) }.toSeq.unzip
      val details = Seq(
        if(outputs.size > count){
          Some(s"(${df.count} rows total)")
        } else { None }
      ).flatten
      val formatted = formatTable( fields +: outputs )
      def printSeparator() = 
        println(
          "+-"+
          formatted.head
                   .map { field => "-" * field.size }
                   .mkString("-+-")
          +"-+"
        )
      printSeparator()
      println("| "+formatted.head.mkString(" | ") + " |")
      printSeparator()
      for((row, caveatNote) <- formatted.tail.zip(caveatNotes)){
        println(
          "| "+
          row.mkString(" | ")+
          " |"+
          caveatNote.getOrElse(""))
      }
      printSeparator()
      for(detail <- details){
        println(detail)
      }
    }

    def printRaw(msg: Array[Byte])
    {
      println(msg.mkString)
    }

  }

  class PrettyOutputFormat(terminal: Terminal)
    extends OutputFormat
  {

    def print(msg: String)
    {
      terminal.writer.write(msg)
      if(msg.length <= 0  || msg.charAt(msg.length - 1) != '\n'){ terminal.writer.write('\n') }
    }


    def print(df: DataFrame, count: Int)
    {
      val fields: Seq[String] = 
        df.schema.fieldNames.dropRight(1)

      val (outputs, caveatNotes): (Seq[Seq[String]], Seq[(Seq[Boolean], Boolean)]) = 
        df.take(count+1)
          .map { row => 
            val (values, caveats): (Seq[Any], Seq[Boolean]) = 
              row.caveattedAttributes.unzip
            (
              values.map { case null => "null"; case x => x.toString }, 
              (caveats, row.isCaveatted)
            )
          }
          .toSeq.unzip

      val details = Seq(
        if(outputs.size > count){
          Some(s"(${df.count} rows total)")
        } else { None }
      ).flatten

      val formatted = formatTable( fields +: outputs )

      def printSeparator() = 
        terminal.writer.write(
          "+-"+
          formatted.head
                   .map { field => "-" * field.size }
                   .mkString("-+-")
          +"-+\n"
        )

      printSeparator()
      terminal.writer.write("| "+formatted.head.mkString(" | ") + " |\n")
      printSeparator()

      for((row, (attrCaveats, rowCaveat)) <- formatted.tail.zip(caveatNotes)){
        val lineStyle = 
          if(rowCaveat){ 
            AttributedStyle.DEFAULT.faint().underline() 
          } else { 
            AttributedStyle.DEFAULT 
          }
        val line = new AttributedStringBuilder(200)
        var sep: String = "| "
        for( (valueStr, valueIsCaveatted) <- row.zip(attrCaveats) ){
          line.append(sep, lineStyle)
          line.append(valueStr, 
            if(valueIsCaveatted){ 
              lineStyle.foreground(AttributedStyle.RED) 
            } else { lineStyle }
          )
          sep = " | "
        }
        line.append(" |", lineStyle)
        line.append("\n")
        terminal.writer.write(
          line.toAttributedString.toAnsi(terminal)
        )
      }
      printSeparator()
      for(detail <- details){
        terminal.writer.write(detail)
      }
      terminal.writer.flush()
    }

    def printRaw(msg: Array[Byte])
    {
      terminal.output.write(msg)
    }
  }
}