package samza.examples.cookbook;

import java.io.Serializable;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
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


public class SessionWindowExample implements StreamApplication, Serializable {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String INPUT_STREAM_ID = "pageview-session-input";
  private static final String OUTPUT_STREAM_ID = "pageview-session-output";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    Serde<String> stringSerde = new StringSerde();
    KVSerde<String, PageView> pageViewKVSerde = KVSerde.of(stringSerde, new JsonSerdeV2<>(PageView.class));
    KVSerde<String, UserPageViews> userPageViewSerde = KVSerde.of(stringSerde, new JsonSerdeV2<>(UserPageViews.class));

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<KV<String, PageView>> pageViewInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(INPUT_STREAM_ID, pageViewKVSerde);
    KafkaOutputDescriptor<KV<String, UserPageViews>> userPageViewsOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, userPageViewSerde);

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);

    MessageStream<KV<String, PageView>> pageViews = appDescriptor.getInputStream(pageViewInputDescriptor);
    OutputStream<KV<String, UserPageViews>> userPageViews = appDescriptor.getOutputStream(userPageViewsOutputDescriptor);

    pageViews
        .partitionBy(kv -> kv.value.userId, kv -> kv.value, pageViewKVSerde, "pageview")
        .window(Windows.keyedSessionWindow(kv -> kv.value.userId,
            Duration.ofSeconds(10), stringSerde, pageViewKVSerde), "usersession")
        .map(windowPane -> {
          String userId = windowPane.getKey().getKey();
          int views = windowPane.getMessage().size();
          return KV.of(userId, new UserPageViews(userId, views));
        })
        .sendTo(userPageViews);
  }
}
