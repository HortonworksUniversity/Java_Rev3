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
import cascading.flow.FlowRuntimeProps;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.tez.Hadoop2TezFlowConnector;
import cascading.operation.Identity;
import cascading.operation.text.DateFormatter;
import cascading.operation.text.DateParser;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import cascading.pipe.assembly.Coerce;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.FirstBy;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.LeftJoin;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

import com.google.common.collect.Ordering;

public class StockAnalyzer {

  public enum CLI_OPTIONS {
    stocks, dividends, output, local, tez;
  }

  private enum FIELDS {
    exchange, stock_symbol, date, stock_price_open, stock_price_close,
    stock_price_high, stock_price_low, stock_volume, stock_price_adj_close,
    dividends, div_symbol, div_date, max_high, min_low, max_dividend;
  }

  private enum SOURCE_TAP_NAMES {
    stocks, dividends;
  }

  public static Pipe buildStockAnalysisAssembly() {
    DateParser dateParser = new DateParser(new Fields(FIELDS.date.name()),
        "yyyy-MM-dd");
    DateFormatter dateFormatter = new DateFormatter(new Fields(
        FIELDS.date.name()), "yyyy");
    Pipe lhs = new Discard(new Pipe(SOURCE_TAP_NAMES.stocks.name()),
        new Fields(FIELDS.exchange.name(), FIELDS.stock_price_close.name(),
            FIELDS.stock_volume.name(), FIELDS.stock_price_adj_close.name()));

    Pipe rhs = new Discard(new Pipe(SOURCE_TAP_NAMES.dividends.name()),
        new Fields(FIELDS.exchange.name()));
    rhs = new Rename(rhs, new Fields(FIELDS.stock_symbol.name(),
        FIELDS.date.name()), new Fields(FIELDS.div_symbol.name(),
        FIELDS.div_date.name()));

    Pipe assembly = new CoGroup(lhs, new Fields(FIELDS.stock_symbol.name(),
        FIELDS.date.name()), rhs, new Fields(FIELDS.div_symbol.name(),
        FIELDS.div_date.name()), new LeftJoin());

    assembly = new Each(assembly, new Fields(FIELDS.stock_symbol.name(),
        FIELDS.date.name(), FIELDS.stock_price_high.name(),
        FIELDS.stock_price_low.name(), FIELDS.dividends.name()), new Identity());
    assembly = new Coerce(assembly, new Fields(FIELDS.stock_price_high.name(),
        FIELDS.stock_price_low.name(), FIELDS.dividends.name()), double.class,
        double.class, double.class);

    assembly = new Each(assembly, new Fields(FIELDS.date.name()), dateParser,
        Fields.REPLACE);
    assembly = new Each(assembly, new Fields(FIELDS.date.name()),
        dateFormatter, Fields.REPLACE);

    Fields groupingFields = new Fields(FIELDS.stock_symbol.name(),
        FIELDS.date.name());

    Fields high = new Fields(FIELDS.stock_price_high.name());
    high.setComparators(Ordering.natural().reverse());
    FirstBy maxHigh = new FirstBy(high, new Fields(FIELDS.max_high.name()));

    Fields low = new Fields(FIELDS.stock_price_low.name());
    low.setComparators(Ordering.natural());
    FirstBy minLow = new FirstBy(low, new Fields(FIELDS.min_low.name()));

    Fields dividends = new Fields(FIELDS.dividends.name());
    dividends.setComparators(Ordering.natural().reverse());
    FirstBy maxDividend = new FirstBy(dividends, new Fields(
        FIELDS.max_dividend.name()));

    assembly = new AggregateBy(assembly, groupingFields, maxHigh, minLow,
        maxDividend);

    return assembly;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void main(String[] args) throws ParseException {

    Options options = new Options();
    options.addOption(new Option(CLI_OPTIONS.stocks.name(), true,
        "Stocks input path for job"));
    options.addOption(new Option(CLI_OPTIONS.dividends.name(), true,
        "Dividends input path for job"));
    options.addOption(new Option(CLI_OPTIONS.output.name(), true,
        "Output path for job"));
    options.addOption(new Option(CLI_OPTIONS.local.name(), false,
        "Run locally?"));
    options.addOption(new Option(CLI_OPTIONS.tez.name(), false,
        "Run with Tez?"));
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);
    HelpFormatter help = new HelpFormatter();
    if (!cmd.hasOption(CLI_OPTIONS.stocks.name())
        || !cmd.hasOption(CLI_OPTIONS.dividends.name())
        || !cmd.hasOption(CLI_OPTIONS.output.name())) {
      help.printHelp("<cascading jar>", options);
      System.exit(1);
    }

    String stocksPath = cmd.getOptionValue(CLI_OPTIONS.stocks.name());
    String dividendsPath = cmd.getOptionValue(CLI_OPTIONS.dividends.name());
    String outputPath = cmd.getOptionValue(CLI_OPTIONS.output.name());
    boolean local = cmd.hasOption(CLI_OPTIONS.local.name());
    boolean tez = cmd.hasOption(CLI_OPTIONS.tez.name());

    Properties properties = new Properties();
    AppProps.setApplicationJarClass(properties, StockAnalyzer.class);
    FlowConnector flowConnector = null;
    Tap stocksSource = null;
    Tap dividendsSource = null;
    Tap sink = null;

    if (local) {
      Scheme stockSourceScheme = new cascading.scheme.local.TextDelimited(true,
          ",");
      Scheme dividendSourceScheme = new cascading.scheme.local.TextDelimited(
          true, ",");
      stocksSource = new FileTap(stockSourceScheme, stocksPath);
      dividendsSource = new FileTap(dividendSourceScheme, dividendsPath);
      Scheme sinkScheme = new cascading.scheme.local.TextDelimited(true, ",");
      sink = new FileTap(sinkScheme, outputPath, SinkMode.REPLACE);
      flowConnector = new LocalFlowConnector(properties);
    }
    else {
      Scheme stockSourceScheme = new TextDelimited(true, ",");
      Scheme dividendSourceScheme = new TextDelimited(true, ",");
      stocksSource = new Hfs(stockSourceScheme, stocksPath);
      dividendsSource = new Hfs(dividendSourceScheme, dividendsPath);
      Scheme sinkScheme = new TextDelimited(false, ",");
      sink = new Hfs(sinkScheme, outputPath, SinkMode.REPLACE);
      if (tez) {
        properties.put("tez.lib.uris",
            "hdfs:///apps/tez-0.5.0/tez-0.5.0.tar.gz");
        properties = FlowRuntimeProps.flowRuntimeProps().setGatherPartitions(4)
            .buildProperties(properties);
        flowConnector = new Hadoop2TezFlowConnector(properties);
      }
      else {
        flowConnector = new Hadoop2MR1FlowConnector(properties);
      }
    }

    FlowDef def = new FlowDef()
        .addSource(SOURCE_TAP_NAMES.stocks.name(), stocksSource)
        .addSource(SOURCE_TAP_NAMES.dividends.name(), dividendsSource)
        .addTailSink(buildStockAnalysisAssembly(), sink)
        .setName("stock-analyzer");

    Flow flow = flowConnector.connect(def);
    flow.complete();
  }
}
