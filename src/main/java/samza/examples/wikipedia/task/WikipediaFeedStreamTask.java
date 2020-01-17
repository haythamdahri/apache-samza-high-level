package samza.examples.wikipedia.task;

import java.util.Map;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;


public class WikipediaFeedStreamTask implements StreamTask {
  private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "wikipedia-raw");

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    Map<String, Object> outgoingMap = WikipediaFeedEvent.toMap((WikipediaFeedEvent) envelope.getMessage());
    collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, outgoingMap));
  }
}
