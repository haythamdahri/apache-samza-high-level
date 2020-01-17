package samza.examples.cookbook;

import com.couchbase.client.java.document.json.JsonObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.context.Context;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.remote.NoOpTableReadFunction;
import org.apache.samza.table.remote.RemoteTable;
import org.apache.samza.table.remote.couchbase.CouchbaseTableWriteFunction;
import org.apache.samza.table.retry.TableRetryPolicy;



public class CouchbaseTableExample implements StreamApplication {

  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String INPUT_STREAM_ID = "word-input";
  private static final String OUTPUT_STREAM_ID = "count-output";

  private static final String CLUSTER_NODES = "couchbase://127.0.0.1";
  private static final int COUCHBASE_PORT = 11210;
  private static final String BUCKET_NAME = "my-bucket";
  private static final String BUCKET_PASSWORD = "123456";
  private static final String TOTAL_COUNT_ID = "total-count";

  @Override
  public void describe(StreamApplicationDescriptor app) {

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<String> wordInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, new StringSerde());

    KafkaOutputDescriptor<String> countOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, new StringSerde());

    MyCouchbaseTableWriteFunction writeFn = new MyCouchbaseTableWriteFunction(BUCKET_NAME, CLUSTER_NODES)
        .withBootstrapCarrierDirectPort(COUCHBASE_PORT)
        .withUsernameAndPassword(BUCKET_NAME, BUCKET_PASSWORD)
        .withTimeout(Duration.ofSeconds(5));

    TableRetryPolicy retryPolicy = new TableRetryPolicy()
        .withFixedBackoff(Duration.ofSeconds(1))
        .withStopAfterAttempts(3);

    RemoteTableDescriptor couchbaseTableDescriptor = new RemoteTableDescriptor("couchbase-table")
        .withReadFunction(new NoOpTableReadFunction())
        .withReadRateLimiterDisabled()
        .withWriteFunction(writeFn)
        .withWriteRetryPolicy(retryPolicy)
        .withWriteRateLimit(4);

    app.withDefaultSystem(kafkaSystemDescriptor);
    MessageStream<String> wordStream = app.getInputStream(wordInputDescriptor);
    OutputStream<String> countStream = app.getOutputStream(countOutputDescriptor);
    app.getTable(couchbaseTableDescriptor);

    wordStream
        .flatMap(m -> Arrays.asList(m.split(" ")))
        .filter(word -> word != null && word.length() > 0)
        .map(new MyCountFunction())
        .map(countString -> currentTime() + " " + countString)
        .sendTo(countStream);
  }

  static class MyCountFunction implements MapFunction<String, String> {

    private MyCouchbaseTableWriteFunction writeFn;

    @Override
    public void init(Context context) {
      RemoteTable table = (RemoteTable) context.getTaskContext().getTable("couchbase-table");
      writeFn = (MyCouchbaseTableWriteFunction) table.getWriteFunction();
    }

    @Override
    public String apply(String word) {
      CompletableFuture<Long> countFuture = writeFn.incCounter(word);
      CompletableFuture<Long> totalCountFuture = writeFn.incCounter(TOTAL_COUNT_ID);
      return String.format("%s word=%s, count=%d, total-count=%d",
          currentTime(), word, countFuture.join(), totalCountFuture.join());
    }
  }

  static class MyCouchbaseTableWriteFunction extends CouchbaseTableWriteFunction<JsonObject> {

    private final static int OP_COUNTER = 1;

    public MyCouchbaseTableWriteFunction(String bucketName, String... clusterNodes) {
      super(bucketName, JsonObject.class, clusterNodes);
    }

    @Override
    public <T> CompletableFuture<T> writeAsync(int opId, Object... args) {
      switch (opId) {
        case OP_COUNTER:
          Preconditions.checkArgument(2 == args.length,
              String.format("Two arguments (String and int) are expected for counter operation (opId=%d)", opId));
          String id = (String) args[0];
          int delta = (int) args[1];
          return asyncWriteHelper(
              bucket.async().counter(id, delta, 1, timeout.toMillis(), TimeUnit.MILLISECONDS),
              String.format("Failed to invoke counter with Id %s from bucket %s.", id, bucketName),
              false);
        default:
          throw new SamzaException("Unknown opId: " + opId);
      }
    }

    public CompletableFuture<Long> incCounter(String id) {
      return table.writeAsync(OP_COUNTER, id, 1);
    }

  }

  private static String currentTime() {
    return DATE_FORMAT.format(new Date());
  }

}
