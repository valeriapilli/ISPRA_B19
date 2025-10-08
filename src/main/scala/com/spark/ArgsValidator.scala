package com.spark

import org.apache.commons.cli._

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
    input = if(commandLine.getOptionValue("input").endsWith("/"))
      {commandLine.getOptionValue("input")}
    else
      {commandLine.getOptionValue("input").concat("/")}
    println("Source file: " + input)
    output = if (commandLine.getOptionValue("output").endsWith("/"))
    {
      commandLine.getOptionValue("output")
    }
    else {
      commandLine.getOptionValue("output").concat("/")
    }

    println("Target path: " + output)
  }

}
