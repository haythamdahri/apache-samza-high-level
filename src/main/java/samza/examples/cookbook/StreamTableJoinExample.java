package samza.examples.cookbook;

import java.util.Objects;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.table.Table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import samza.examples.cookbook.data.PageView;
import samza.examples.cookbook.data.Profile;

import java.util.List;
import java.util.Map;


public class StreamTableJoinExample implements StreamApplication {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String PROFILE_STREAM_ID = "profile-table-input";
  private static final String PAGEVIEW_STREAM_ID = "pageview-join-input";
  private static final String OUTPUT_TOPIC = "enriched-pageview-join-output";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    Serde<Profile> profileSerde = new JsonSerdeV2<>(Profile.class);
    Serde<PageView> pageViewSerde = new JsonSerdeV2<>(PageView.class);
    Serde<EnrichedPageView> joinResultSerde = new JsonSerdeV2<>(EnrichedPageView.class);

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<Profile> profileInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(PROFILE_STREAM_ID, profileSerde);
    KafkaInputDescriptor<PageView> pageViewInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(PAGEVIEW_STREAM_ID, pageViewSerde);
    KafkaOutputDescriptor<EnrichedPageView> joinResultOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_TOPIC, joinResultSerde);

    RocksDbTableDescriptor<String, Profile> profileTableDescriptor =
        new RocksDbTableDescriptor<String, Profile>("profile-table", KVSerde.of(new StringSerde(), profileSerde));

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);

    MessageStream<Profile> profileStream = appDescriptor.getInputStream(profileInputDescriptor);
    MessageStream<PageView> pageViewStream = appDescriptor.getInputStream(pageViewInputDescriptor);
    OutputStream<EnrichedPageView> joinResultStream = appDescriptor.getOutputStream(joinResultOutputDescriptor);
    Table<KV<String, Profile>> profileTable = appDescriptor.getTable(profileTableDescriptor);

    profileStream
        .map(profile -> KV.of(profile.userId, profile))
        .sendTo(profileTable);

    pageViewStream
        .partitionBy(pv -> pv.userId, pv -> pv, KVSerde.of(new StringSerde(), pageViewSerde), "join")
        .join(profileTable, new JoinFn())
        .sendTo(joinResultStream);
  }

  private static class JoinFn implements StreamTableJoinFunction<String, KV<String, PageView>, KV<String, Profile>, EnrichedPageView> {
    @Override
    public EnrichedPageView apply(KV<String, PageView> message, KV<String, Profile> record) {
      return record == null ? null :
          new EnrichedPageView(message.getKey(), record.getValue().company, message.getValue().pageId);
    }
    @Override
    public String getMessageKey(KV<String, PageView> message) {
      return message.getKey();
    }
    @Override
    public String getRecordKey(KV<String, Profile> record) {
      return record.getKey();
    }
  }

  static public class EnrichedPageView {

    public final String userId;
    public final String company;
    public final String pageId;

    public EnrichedPageView(String userId, String company, String pageId) {
      this.userId = userId;
      this.company = company;
      this.pageId = pageId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EnrichedPageView that = (EnrichedPageView) o;
      return Objects.equals(userId, that.userId) && Objects.equals(company, that.company) && Objects.equals(pageId,
          that.pageId);
    }
  }

}
