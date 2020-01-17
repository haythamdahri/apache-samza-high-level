package samza.examples.wikipedia.task.application;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;
import samza.examples.wikipedia.system.descriptors.WikipediaInputDescriptor;
import samza.examples.wikipedia.system.descriptors.WikipediaSystemDescriptor;
import samza.examples.wikipedia.task.WikipediaFeedStreamTask;



public class WikipediaFeedTaskApplication implements TaskApplication {

  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  @Override
  public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {

    WikipediaSystemDescriptor wikipediaSystemDescriptor = new WikipediaSystemDescriptor("irc.wikimedia.org", 6667);

    WikipediaInputDescriptor wikipediaInputDescriptor =
        wikipediaSystemDescriptor.getInputDescriptor("en-wikipedia").withChannel("#en.wikipedia");
    WikipediaInputDescriptor wiktionaryInputDescriptor =
        wikipediaSystemDescriptor.getInputDescriptor("en-wiktionary").withChannel("#en.wiktionary");
    WikipediaInputDescriptor wikiNewsInputDescriptor =
        wikipediaSystemDescriptor.getInputDescriptor("en-wikinews").withChannel("#en.wikinews");

    KafkaSystemDescriptor kafkaSystemDescriptor =
        new KafkaSystemDescriptor("kafka").withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
            .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
            .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaOutputDescriptor kafkaOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor("wikipedia-raw", new JsonSerde<>());

    taskApplicationDescriptor.withDefaultSystem(kafkaSystemDescriptor);

    taskApplicationDescriptor.withInputStream(wikipediaInputDescriptor);
    taskApplicationDescriptor.withInputStream(wiktionaryInputDescriptor);
    taskApplicationDescriptor.withInputStream(wikiNewsInputDescriptor);

    taskApplicationDescriptor.withOutputStream(kafkaOutputDescriptor);

    taskApplicationDescriptor.withTaskFactory((StreamTaskFactory) () -> new WikipediaFeedStreamTask());
  }
}
