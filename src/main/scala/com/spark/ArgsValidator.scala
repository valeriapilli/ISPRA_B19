package com.spark

import org.apache.commons.cli.{BasicParser, CommandLine, CommandLineParser, HelpFormatter, Option, Options, ParseException}

object ArgsValidator {

  var input: String = _
  var output: String = _

  def checkArgs(args: Array[String]): Unit = {
    val allOptions: Options = new Options()

    val inputPath = new Option("input", "in", true, "input file")
      inputPath.setArgName("input")
      //inputPath.setRequired(true)
      allOptions.addOption(inputPath)

    val outputPath = new Option("output", "out", true, "output path")
    inputPath.setArgName("output")
    inputPath.setRequired(true)
    allOptions.addOption(outputPath)

    val commandLineParser: CommandLineParser = new BasicParser()

    val commandLine: CommandLine = commandLineParser.parse(allOptions, args)
    input = commandLine.getOptionValue("input")
    println("Source file: " + input)
    output = commandLine.getOptionValue("output")
    println("Target path" + output)
  }

}
