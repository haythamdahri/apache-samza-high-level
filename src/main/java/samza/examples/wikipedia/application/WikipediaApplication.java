
package samza.examples.wikipedia.application;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.Objects;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.context.Context;
import org.apache.samza.context.TaskContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.Serde;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import samza.examples.wikipedia.model.WikipediaParser;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;
import samza.examples.wikipedia.system.descriptors.WikipediaInputDescriptor;
import samza.examples.wikipedia.system.descriptors.WikipediaSystemDescriptor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class WikipediaApplication implements StreamApplication, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(WikipediaApplication.class);

  private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("localhost:2181");
  private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("localhost:9092");
  private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

  public static final String WIKIPEDIA_CHANNEL = "#en.wikipedia";
  public static final String WIKINEWS_CHANNEL = "#en.wikinews";
  public static final String WIKTIONARY_CHANNEL = "#en.wiktionary";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {

    Duration windowDuration =
        appDescriptor.getConfig().containsKey("deploy.test") ? Duration.ofMillis(10) : Duration.ofSeconds(10);
    WikipediaSystemDescriptor wikipediaSystemDescriptor = new WikipediaSystemDescriptor("irc.wikimedia.org", 6667);

    WikipediaInputDescriptor wikipediaInputDescriptor = wikipediaSystemDescriptor
        .getInputDescriptor("en-wikipedia")
        .withChannel(WIKIPEDIA_CHANNEL);
    WikipediaInputDescriptor wiktionaryInputDescriptor = wikipediaSystemDescriptor
        .getInputDescriptor("en-wiktionary")
        .withChannel(WIKTIONARY_CHANNEL);
    WikipediaInputDescriptor wikiNewsInputDescriptor = wikipediaSystemDescriptor
        .getInputDescriptor("en-wikinews")
        .withChannel(WIKINEWS_CHANNEL);

    KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor("kafka")
        .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

    KafkaOutputDescriptor<WikipediaStatsOutput> statsOutputDescriptor =
        kafkaSystemDescriptor.getOutputDescriptor("wikipedia-stats", new JsonSerdeV2<>(WikipediaStatsOutput.class));

    appDescriptor.withDefaultSystem(kafkaSystemDescriptor);
    MessageStream<WikipediaFeedEvent> wikipediaEvents = appDescriptor.getInputStream(wikipediaInputDescriptor);
    MessageStream<WikipediaFeedEvent> wiktionaryEvents = appDescriptor.getInputStream(wiktionaryInputDescriptor);
    MessageStream<WikipediaFeedEvent> wikiNewsEvents = appDescriptor.getInputStream(wikiNewsInputDescriptor);
    OutputStream<WikipediaStatsOutput> wikipediaStats = appDescriptor.getOutputStream(statsOutputDescriptor);

    MessageStream<WikipediaFeedEvent> allWikipediaEvents =
        MessageStream.mergeAll(ImmutableList.of(wikipediaEvents, wiktionaryEvents, wikiNewsEvents));


    allWikipediaEvents
        .map(WikipediaParser::parseEvent)
        .window(Windows.tumblingWindow(windowDuration,
            WikipediaStats::new, new WikipediaStatsAggregator(), WikipediaStats.serde()), "statsWindow")
        .map(this::formatOutput)
        .sendTo(wikipediaStats);
  }


  private static class WikipediaStatsAggregator implements FoldLeftFunction<Map<String, Object>, WikipediaStats> {
    private static final String EDIT_COUNT_KEY = "count-edits-all-time";

    private transient KeyValueStore<String, Integer> store;

    private transient Counter repeatEdits;


    @Override
    public void init(Context context) {
      TaskContext taskContext = context.getTaskContext();
      store = (KeyValueStore<String, Integer>) taskContext.getStore("wikipedia-stats");
      repeatEdits = taskContext.getTaskMetricsRegistry().newCounter("edit-counters", "repeat-edits");
    }

    @Override
    public WikipediaStats apply(Map<String, Object> edit, WikipediaStats stats) {

      Integer editsAllTime = store.get(EDIT_COUNT_KEY);
      if (editsAllTime == null) {
        editsAllTime = 0;
      }
      editsAllTime++;
      store.put(EDIT_COUNT_KEY, editsAllTime);

      stats.edits++;
      stats.totalEdits = editsAllTime;
      stats.byteDiff += (Integer) edit.get("diff-bytes");
      boolean newTitle = stats.titles.add((String) edit.get("title"));

      Map<String, Boolean> flags = (Map<String, Boolean>) edit.get("flags");
      for (Map.Entry<String, Boolean> flag : flags.entrySet()) {
        if (Boolean.TRUE.equals(flag.getValue())) {
          stats.counts.compute(flag.getKey(), (k, v) -> v == null ? 0 : v + 1);
        }
      }

      if (!newTitle) {
        repeatEdits.inc();
        LOG.info("Frequent edits for title: {}", edit.get("title"));
      }
      return stats;
    }
  }

  private WikipediaStatsOutput formatOutput(WindowPane<Void, WikipediaStats> statsWindowPane) {
    WikipediaStats stats = statsWindowPane.getMessage();
    return new WikipediaStatsOutput(stats.edits, stats.totalEdits, stats.byteDiff, stats.titles.size(), stats.counts);
  }

 
  public static class WikipediaStats {
    int edits = 0;
    int byteDiff = 0;
    Set<String> titles = new HashSet<>();
    Map<String, Integer> counts = new HashMap<>();

    int totalEdits = 0;

    @Override
    public String toString() {
      return String.format("Stats {edits:%d, byteDiff:%d, titles:%s, counts:%s}", edits, byteDiff, titles, counts);
    }

    static Serde<WikipediaStats> serde() {
      return new WikipediaStatsSerde();
    }

    public static class WikipediaStatsSerde implements Serde<WikipediaStats> {
      @Override
      public WikipediaStats fromBytes(byte[] bytes) {
        try {
          ByteArrayInputStream bias = new ByteArrayInputStream(bytes);
          ObjectInputStream ois = new ObjectInputStream(bias);
          WikipediaStats stats = new WikipediaStats();
          stats.edits = ois.readInt();
          stats.byteDiff = ois.readInt();
          stats.titles = (Set<String>) ois.readObject();
          stats.counts = (Map<String, Integer>) ois.readObject();
          return stats;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public byte[] toBytes(WikipediaStats wikipediaStats) {
        try {
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          ObjectOutputStream dos = new ObjectOutputStream(baos);
          dos.writeInt(wikipediaStats.edits);
          dos.writeInt(wikipediaStats.byteDiff);
          dos.writeObject(wikipediaStats.titles);
          dos.writeObject(wikipediaStats.counts);
          return baos.toByteArray();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  static class WikipediaStatsOutput {
    public int edits;
    public int editsAllTime;
    public int bytesAdded;
    public int uniqueTitles;
    public Map<String, Integer> counts;

    public WikipediaStatsOutput(int edits, int editsAllTime, int bytesAdded, int uniqueTitles,
        Map<String, Integer> counts) {
      this.edits = edits;
      this.editsAllTime = editsAllTime;
      this.bytesAdded = bytesAdded;
      this.uniqueTitles = uniqueTitles;
      this.counts = counts;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      WikipediaStatsOutput that = (WikipediaStatsOutput) o;
      return edits == that.edits && editsAllTime == that.editsAllTime && bytesAdded == that.bytesAdded
          && uniqueTitles == that.uniqueTitles && Objects.equals(counts, that.counts);
    }

    @Override
    public String toString() {
      return "WikipediaStatsOutput{" + "edits=" + edits + ", editsAllTime=" + editsAllTime + ", bytesAdded="
          + bytesAdded + ", uniqueTitles=" + uniqueTitles + ", counts=" + counts + '}';
    }

  }
}

