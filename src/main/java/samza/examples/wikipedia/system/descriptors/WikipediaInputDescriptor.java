package samza.examples.wikipedia.system.descriptors;

import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.descriptors.InputDescriptor;
import org.apache.samza.system.descriptors.SystemDescriptor;

import samza.examples.wikipedia.system.WikipediaFeed;


public class WikipediaInputDescriptor extends InputDescriptor<WikipediaFeed.WikipediaFeedEvent, WikipediaInputDescriptor> {
  private static final Serde SERDE = new NoOpSerde();

  WikipediaInputDescriptor(String streamId, SystemDescriptor systemDescriptor) {
    super(streamId, SERDE, systemDescriptor, null);
  }

  public WikipediaInputDescriptor withChannel(String channel) {
    withPhysicalName(channel);
    return this;
  }
}
