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
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.aggregator.MinValue;
import cascading.operation.aggregator.Sum;
import cascading.operation.text.DateFormatter;
import cascading.operation.text.DateParser;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.assembly.Rename;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class StockAnalyzer {

  public enum CLI_OPTIONS {
    stocks, dividends, output, local;
  }

  private enum FIELDS {
    exchange, stock_symbol, date, stock_price_open, stock_price_close, stock_price_high, stock_price_low, stock_volume,
    stock_price_adj_close, dividends, div_symbol, div_date, max_high, min_low, avg_close, sum_dividend;
  }

  private enum SOURCE_TAP_NAMES {
    stocks, dividends;
  }

  public static Pipe buildStockAnalysisAssembly() {
    DateParser dateParser = new DateParser(new Fields(FIELDS.date.name()), "yyyy-MM-dd");
    DateFormatter dateFormatter = new DateFormatter(new Fields(FIELDS.date.name()), "yyyy");
    Pipe lhs = new Discard(new Pipe(SOURCE_TAP_NAMES.stocks.name()), new Fields(FIELDS.exchange.name(), FIELDS.stock_volume.name(),
        FIELDS.stock_price_adj_close.name()));
    lhs = new Each(lhs, new Fields(FIELDS.date.name()), dateParser, Fields.REPLACE);
    lhs = new Each(lhs, new Fields(FIELDS.date.name()), dateFormatter, Fields.REPLACE);

    Pipe rhs = new Discard(new Pipe(SOURCE_TAP_NAMES.dividends.name()), new Fields(FIELDS.exchange.name()));
    rhs = new Each(rhs, new Fields(FIELDS.date.name()), dateParser, Fields.REPLACE);
    rhs = new Each(rhs, new Fields(FIELDS.date.name()), dateFormatter, Fields.REPLACE);
    rhs = new Rename(rhs, new Fields(FIELDS.stock_symbol.name(), FIELDS.date.name()), new Fields(FIELDS.div_symbol.name(),
        FIELDS.div_date.name()));

    Pipe assembly = new CoGroup(lhs, new Fields(FIELDS.stock_symbol.name(), FIELDS.date.name()), rhs, new Fields(
        FIELDS.div_symbol.name(), FIELDS.div_date.name()), new InnerJoin());

    assembly = new Every(assembly, new Fields(FIELDS.dividends.name()), new Sum(new Fields(FIELDS.sum_dividend.name())), Fields.ALL);
    assembly = new Every(assembly, new Fields(FIELDS.stock_price_close.name()), new Average(new Fields(FIELDS.avg_close.name())),
        Fields.ALL);
    assembly = new Every(assembly, new Fields(FIELDS.stock_price_high.name()), new MaxValue(new Fields(FIELDS.max_high.name())),
        Fields.ALL);
    assembly = new Every(assembly, new Fields(FIELDS.stock_price_low.name()), new MinValue(new Fields(FIELDS.min_low.name())),
        Fields.ALL);
    assembly = new Discard(assembly, new Fields(FIELDS.div_symbol.name(), FIELDS.div_date.name()));

    /*
     * THIS IS THE PREFERRED APPROACH, BUT IT DOES NOT WORK - needs further investigation
     * 
     * assembly = new Each(assembly, new Fields(FIELDS.stock_symbol.name(),
     * FIELDS.date.name(), FIELDS.stock_price_high.name(),
     * FIELDS.stock_price_low.name(), FIELDS.stock_price_close.name(),
     * FIELDS.dividends.name()), new Identity()); Fields groupingFields = new
     * Fields(FIELDS.stock_symbol.name(), FIELDS.date.name()); Fields high = new
     * Fields(FIELDS.stock_price_high.name(), Double.class);
     * high.setComparator(FIELDS.stock_price_high.name(),
     * Collections.reverseOrder(Ordering.natural())); FirstBy maxHigh = new
     * FirstBy(high, new Fields(FIELDS.max_high.name())); Fields low = new
     * Fields(FIELDS.stock_price_low.name(), Double.class);
     * low.setComparator(FIELDS.stock_price_low.name(), Ordering.natural());
     * FirstBy minLow = new FirstBy(low, new Fields(FIELDS.min_low.name()));
     * Fields dividends = new Fields(FIELDS.dividends.name());
     * dividends.setComparator(FIELDS.dividends.name(), Ordering.natural());
     * Fields close = new Fields(FIELDS.stock_price_close.name());
     * close.setComparator(FIELDS.stock_price_close.name(), Ordering.natural());
     * SumBy totalDividends = new SumBy(dividends, new
     * Fields(FIELDS.sum_dividend.name()), Double.class); AverageBy averageClose
     * = new AverageBy(close, new Fields(FIELDS.avg_close.name())); assembly =
     * new AggregateBy(assembly, groupingFields, maxHigh, minLow,
     * totalDividends, averageClose);
     */

    return assembly;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static void main(String[] args) throws ParseException {

    Options options = new Options();
    options.addOption(new Option(CLI_OPTIONS.stocks.name(), true, "Stocks input path for job"));
    options.addOption(new Option(CLI_OPTIONS.dividends.name(), true, "Dividends input path for job"));
    options.addOption(new Option(CLI_OPTIONS.output.name(), true, "Output path for job"));
    options.addOption(new Option(CLI_OPTIONS.local.name(), false, "Run locally?"));
    CommandLineParser parser = new BasicParser();
    CommandLine cmd = parser.parse(options, args);
    HelpFormatter help = new HelpFormatter();
    if (!cmd.hasOption(CLI_OPTIONS.stocks.name()) || !cmd.hasOption(CLI_OPTIONS.dividends.name())
        || !cmd.hasOption(CLI_OPTIONS.output.name())) {
      help.printHelp("<cascading jar>", options);
      System.exit(1);
    }

    String stocksPath = cmd.getOptionValue(CLI_OPTIONS.stocks.name());
    String dividendsPath = cmd.getOptionValue(CLI_OPTIONS.dividends.name());
    String outputPath = cmd.getOptionValue(CLI_OPTIONS.output.name());
    boolean local = cmd.hasOption(CLI_OPTIONS.local.name());

    Properties properties = new Properties();
    AppProps.setApplicationJarClass(properties, StockAnalyzer.class);
    FlowConnector flowConnector = null;
    Tap stocksSource = null;
    Tap dividendsSource = null;
    Tap sink = null;

    if (local) {
      Scheme stockSourceScheme = new cascading.scheme.local.TextDelimited(true, ",");
      Scheme dividendSourceScheme = new cascading.scheme.local.TextDelimited(true, ",");
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
      flowConnector = new HadoopFlowConnector(properties);
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
