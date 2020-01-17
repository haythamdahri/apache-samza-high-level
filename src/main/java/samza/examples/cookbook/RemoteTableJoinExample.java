package samza.examples.cookbook;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.CachingTableDescriptor;
import org.apache.samza.table.remote.BaseTableFunction;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.util.ExponentialSleepStrategy;
import org.apache.samza.util.HttpUtil;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.annotate.JsonProperty;


public class RemoteTableJoinExample implements StreamApplication {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String API_KEY = "demo";

  private static final String URL_TEMPLATE =
      "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=%s&apikey=" + API_KEY;

  private static final String INPUT_STREAM_ID = "stock-symbol-input";
  private static final String OUTPUT_STREAM_ID = "stock-price-output";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<String> stockSymbolInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, new StringSerde());
    KafkaOutputDescriptor<StockPrice> stockPriceOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, new JsonSerdeV2<>(StockPrice.class));
    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);
    MessageStream<String> stockSymbolStream = appDescriptor.getInputStream(stockSymbolInputDescriptor);
    OutputStream<StockPrice> stockPriceStream = appDescriptor.getOutputStream(stockPriceOutputDescriptor);

    RemoteTableDescriptor<String, Double> remoteTableDescriptor =
        new RemoteTableDescriptor("remote-table")
            .withReadRateLimit(10)
            .withReadFunction(new StockPriceReadFunction());
    CachingTableDescriptor<String, Double> cachedRemoteTableDescriptor =
        new CachingTableDescriptor<>("cached-remote-table", remoteTableDescriptor)
            .withReadTtl(Duration.ofSeconds(5));
    Table<KV<String, Double>> cachedRemoteTable = appDescriptor.getTable(cachedRemoteTableDescriptor);

    stockSymbolStream
        .map(symbol -> new KV<String, Void>(symbol, null))
        .join(cachedRemoteTable, new JoinFn())
        .sendTo(stockPriceStream);

  }

  static class JoinFn implements StreamTableJoinFunction<String, KV<String, Void>, KV<String, Double>, StockPrice> {
    @Override
    public StockPrice apply(KV<String, Void> message, KV<String, Double> record) {
      return record == null ? null : new StockPrice(message.getKey(), record.getValue());
    }
    @Override
    public String getMessageKey(KV<String, Void> message) {
      return message.getKey();
    }
    @Override
    public String getRecordKey(KV<String, Double> record) {
      return record.getKey();
    }
  }

  static class StockPriceReadFunction extends BaseTableFunction
      implements TableReadFunction<String, Double> {
    @Override
    public CompletableFuture<Double> getAsync(String symbol) {
      return CompletableFuture.supplyAsync(() -> {
        try {
          URL url = new URL(String.format(URL_TEMPLATE, symbol));
          String response = HttpUtil.read(url, 5000, new ExponentialSleepStrategy());
          JsonParser parser = new JsonFactory().createJsonParser(response);
          while (!parser.isClosed()) {
            if (JsonToken.FIELD_NAME.equals(parser.nextToken()) && "4. close".equalsIgnoreCase(parser.getCurrentName())) {
              return Double.valueOf(parser.nextTextValue());
            }
          }
          return -1d;
        } catch (Exception ex) {
          throw new SamzaException(ex);
        }
      });
    }

    @Override
    public boolean isRetriable(Throwable throwable) {
      return false;
    }
  }

  public static class StockPrice implements Serializable {

    public final String symbol;
    public final Double close;

    public StockPrice(
        @JsonProperty("symbol") String symbol,
        @JsonProperty("close") Double close) {
      this.symbol = symbol;
      this.close = close;
    }
  }

}
