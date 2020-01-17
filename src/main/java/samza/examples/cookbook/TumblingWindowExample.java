package samza.examples.cookbook;

import java.io.Serializable;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import samza.examples.cookbook.data.PageView;
import samza.examples.cookbook.data.UserPageViews;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class TumblingWindowExample implements StreamApplication, Serializable {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String INPUT_STREAM_ID = "pageview-tumbling-input";
  private static final String OUTPUT_STREAM_ID = "pageview-tumbling-output";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KVSerde<String, PageView> pageViewSerde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>(PageView.class));
    KVSerde<String, UserPageViews> userPageViewSerde = KVSerde.of(new StringSerde(), new JsonSerdeV2<>(UserPageViews.class));

    KafkaInputDescriptor<KV<String, PageView>> pageViewInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, pageViewSerde);
    KafkaOutputDescriptor<KV<String, UserPageViews>> userPageViewOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, userPageViewSerde);

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);
    MessageStream<KV<String, PageView>> pageViews = appDescriptor.getInputStream(pageViewInputDescriptor);
    OutputStream<KV<String, UserPageViews>> outputStream = appDescriptor.getOutputStream(userPageViewOutputDescriptor);

    pageViews
        .partitionBy(kv -> kv.value.userId, kv -> kv.value, pageViewSerde, "userId")
        .window(Windows.keyedTumblingWindow(
            kv -> kv.key, Duration.ofSeconds(5), () -> 0, (m, prevCount) -> prevCount + 1,
            new StringSerde(), new IntegerSerde()), "count")
        .map(windowPane -> {
          String userId = windowPane.getKey().getKey();
          int views = windowPane.getMessage();
          return KV.of(userId, new UserPageViews(userId, views));
        })
        .sendTo(outputStream);
  }
}
