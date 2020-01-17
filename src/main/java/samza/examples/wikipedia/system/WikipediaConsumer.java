
package samza.examples.wikipedia.system;

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.Partition;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedListener;

public class WikipediaConsumer extends BlockingEnvelopeMap implements WikipediaFeedListener {
  private final List<String> channels;
  private final String systemName;
  private final WikipediaFeed feed;

  public WikipediaConsumer(String systemName, WikipediaFeed feed, MetricsRegistry registry) {
    this.channels = new ArrayList<String>();
    this.systemName = systemName;
    this.feed = feed;
  }

  public void onEvent(final WikipediaFeedEvent event) {
    SystemStreamPartition systemStreamPartition = new SystemStreamPartition(systemName, event.getChannel(), new Partition(0));

    try {
      put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, null, null, event));
    } catch (Exception e) {
      System.err.println(e);
    }
  }

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String startingOffset) {
    super.register(systemStreamPartition, startingOffset);

    channels.add(systemStreamPartition.getStream());
  }

  @Override
  public void start() {
    feed.start();

    for (String channel : channels) {
      feed.listen(channel, this);
    }
  }

  @Override
  public void stop() {
    for (String channel : channels) {
      feed.unlisten(channel, this);
    }

    feed.stop();
  }
}
