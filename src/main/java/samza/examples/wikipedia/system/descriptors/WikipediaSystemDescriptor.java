package samza.examples.wikipedia.system.descriptors;

import samza.examples.wikipedia.system.WikipediaSystemFactory;

import java.util.Map;
import org.apache.samza.system.descriptors.SystemDescriptor;

public class WikipediaSystemDescriptor extends SystemDescriptor<WikipediaSystemDescriptor> {
  private static final String SYSTEM_NAME = "wikipedia";
  private static final String FACTORY_CLASS_NAME = WikipediaSystemFactory.class.getName();
  private static final String HOST_KEY = "systems.%s.host";
  private static final String PORT_KEY = "systems.%s.port";

  private final String host;
  private final int port;

  public WikipediaSystemDescriptor(String host, int port) {
    super(SYSTEM_NAME, FACTORY_CLASS_NAME, null, null);
    this.host = host;
    this.port = port;
  }

  public WikipediaInputDescriptor getInputDescriptor(String streamId) {
    return new WikipediaInputDescriptor(streamId, this);
  }

  @Override
  public Map<String, String> toConfig() {
    Map<String, String> configs = super.toConfig();
    configs.put(String.format(HOST_KEY, getSystemName()), host);
    configs.put(String.format(PORT_KEY, getSystemName()), Integer.toString(port));
    return configs;
  }
}
