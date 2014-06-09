package cascading;

import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class WordCount {

  public enum CLI_OPTIONS {
    input, output, local;
  }

  private enum FIELDS {
    num, line, word, count;
  }

  @SuppressWarnings("rawtypes")
  public static Pipe buildWordCountAssembly() {
    Pipe assembly = new Pipe("wordcount");
    String regex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";
    Function function = new RegexGenerator(new Fields("word"), regex);
    assembly = new Each(assembly, new Fields("line"), function);
    assembly = new GroupBy(assembly, new Fields("word"));
    Aggregator count = new Count(new Fields("count"));
    assembly = new Every(assembly, count);
    return assembly;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void main(String[] args) throws ParseException {

    Options options = new Options();
    options.addOption(new Option(CLI_OPTIONS.input.name(), true, "Input path for job"));
    options.addOption(new Option(CLI_OPTIONS.output.name(), true, "Output path for job"));
    options.addOption(new Option(CLI_OPTIONS.local.name(), false, "Run locally?"));
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);
    HelpFormatter help = new HelpFormatter();
    if (!cmd.hasOption(CLI_OPTIONS.input.name()) || !cmd.hasOption(CLI_OPTIONS.output.name())) {
      help.printHelp("<cascading jar>", options);
      System.exit(1);
    }

    String inputPath = cmd.getOptionValue(CLI_OPTIONS.input.name());
    String outputPath = cmd.getOptionValue(CLI_OPTIONS.output.name());
    boolean local = cmd.hasOption(CLI_OPTIONS.local.name());

    Properties properties = new Properties();
    AppProps.setApplicationJarClass(properties, WordCount.class);
    FlowConnector flowConnector = null;
    Tap source = null;
    Tap sink = null;
    if (local) {
      Scheme sourceScheme = new cascading.scheme.local.TextLine(new Fields(FIELDS.num.name(), FIELDS.line.name()));
      source = new FileTap(sourceScheme, inputPath);
      Scheme sinkScheme = new cascading.scheme.local.TextLine(new Fields(FIELDS.word.name(), FIELDS.count.name()));
      sink = new FileTap(sinkScheme, outputPath, SinkMode.REPLACE);
      flowConnector = new LocalFlowConnector(properties);
    }
    else {
      Scheme sourceScheme = new TextLine(new Fields(FIELDS.num.name(), FIELDS.line.name()));
      source = new Hfs(sourceScheme, inputPath);
      Scheme sinkScheme = new TextLine(new Fields(FIELDS.word.name(), FIELDS.count.name()));
      sink = new Hfs(sinkScheme, outputPath, SinkMode.REPLACE);
      flowConnector = new HadoopFlowConnector(properties);
    }

    FlowDef def = new FlowDef()
    .addSource("wordcount", source)
    .addTailSink(buildWordCountAssembly(), sink)
    .setName("word-count");
    
    Flow flow = flowConnector.connect(def);
    flow.complete();
  }
}
