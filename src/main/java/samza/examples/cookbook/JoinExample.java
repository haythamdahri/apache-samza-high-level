package samza.examples.cookbook;

import java.io.Serializable;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import samza.examples.cookbook.data.AdClick;
import samza.examples.cookbook.data.PageView;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class JoinExample implements StreamApplication, Serializable {
  private static final String KAFKA_SYSTEM_NAME = "kafka";
  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  private static final String PAGEVIEW_STREAM_ID = "pageview-join-input";
  private static final String ADCLICK_STREAM_ID = "adclick-join-input";
  private static final String OUTPUT_STREAM_ID = "pageview-adclick-join-output";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    StringSerde stringSerde = new StringSerde();
    JsonSerdeV2<PageView> pageViewSerde = new JsonSerdeV2<>(PageView.class);
    JsonSerdeV2<AdClick> adClickSerde = new JsonSerdeV2<>(AdClick.class);
    JsonSerdeV2<JoinResult> joinResultSerde = new JsonSerdeV2<>(JoinResult.class);

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaInputDescriptor<PageView> pageViewInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(PAGEVIEW_STREAM_ID, pageViewSerde);
    KafkaInputDescriptor<AdClick> adClickInputDescriptor =
        kafkaSystemDescriptor.getInputDescriptor(ADCLICK_STREAM_ID, adClickSerde);
    KafkaOutputDescriptor<JoinResult> joinResultOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, joinResultSerde);

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);

    MessageStream<PageView> pageViews = appDescriptor.getInputStream(pageViewInputDescriptor);
    MessageStream<AdClick> adClicks = appDescriptor.getInputStream(adClickInputDescriptor);
    OutputStream<JoinResult> joinResults = appDescriptor.getOutputStream(joinResultOutputDescriptor);

    JoinFunction<String, PageView, AdClick, JoinResult> pageViewAdClickJoinFunction =
        new JoinFunction<String, PageView, AdClick, JoinResult>() {
          @Override
          public JoinResult apply(PageView pageView, AdClick adClick) {
            return new JoinResult(pageView.pageId, pageView.userId, pageView.country, adClick.getAdId());
          }

          @Override
          public String getFirstKey(PageView pageView) {
            return pageView.pageId;
          }

          @Override
          public String getSecondKey(AdClick adClick) {
            return adClick.getPageId();
          }
        };

    MessageStream<PageView> repartitionedPageViews =
        pageViews
            .partitionBy(pv -> pv.pageId, pv -> pv, KVSerde.of(stringSerde, pageViewSerde), "pageview")
            .map(KV::getValue);

    MessageStream<AdClick> repartitionedAdClicks =
        adClicks
            .partitionBy(AdClick::getPageId, ac -> ac, KVSerde.of(stringSerde, adClickSerde), "adclick")
            .map(KV::getValue);

    repartitionedPageViews
        .join(repartitionedAdClicks, pageViewAdClickJoinFunction,
            stringSerde, pageViewSerde, adClickSerde, Duration.ofMinutes(3), "join")
        .sendTo(joinResults);
  }

  static class JoinResult {
    public String pageId;
    public String userId;
    public String country;
    public String adId;

    public JoinResult(String pageId, String userId, String country, String adId) {
      this.pageId = pageId;
      this.userId = userId;
      this.country = country;
      this.adId = adId;
    }
  }
}
