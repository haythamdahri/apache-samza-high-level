
package samza.examples.wikipedia.system;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;

public class WikipediaSystemFactory implements SystemFactory {
  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SinglePartitionWithoutOffsetsSystemAdmin();
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    String host = config.get("systems." + systemName + ".host");
    int port = config.getInt("systems." + systemName + ".port");
    WikipediaFeed feed = new WikipediaFeed(host, port);

    return new WikipediaConsumer(systemName, feed, registry);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    throw new SamzaException("You can't produce to a Wikipedia feed! How about making some edits to a Wiki, instead?");
  }
}
