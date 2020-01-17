package samza.examples.wikipedia.task.test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;
import samza.examples.wikipedia.task.application.WikipediaParserTaskApplication;

public class TestWikipediaTask {

  @Test
  public void testWikipediaFeedTask() throws Exception {
    String[] wikipediaFeedSamples = new String[] { "{\"channel\":\"#en.wikipedia\",\"raw\":\"[[Fear Is the Key (song)]]  https://en.wikipedia.org/w/index.php?diff=865574761&oldid=861177329 * Sam Sailor * (+46) Redirecting to [[Fear of the Dark (Iron Maiden album)]] ([[User:Sam Sailor/Scripts/Sagittarius+|♐]])\",\"time\":1540408899419,\"source\":\"rc-pmtpa\"}" };

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("kafka");

    InMemoryInputDescriptor rawWikiEvents = isd
        .getInputDescriptor("wikipedia-raw", new NoOpSerde<>());

    InMemoryOutputDescriptor<WikipediaFeedEvent> outputStreamDesc = isd
        .getOutputDescriptor("wikipedia-edits", new NoOpSerde<>());

    TestRunner
        .of(new WikipediaParserTaskApplication())
        .addInputStream(rawWikiEvents, parseJSONToMap(wikipediaFeedSamples))
        .addOutputStream(outputStreamDesc, 1)
        .run(Duration.ofSeconds(2));

    Assert.assertEquals(1
        , TestRunner.consumeStream(outputStreamDesc, Duration.ofSeconds(1)).get(0).size());
  }

  public static List<Map<String, Object>> parseJSONToMap(String[] lines) throws Exception{
    List<Map<String, Object>> wikiRawEvents = new ArrayList<>();
    ObjectMapper mapper = new ObjectMapper();
    for (String line : lines) {
      wikiRawEvents.add(mapper.readValue(line, HashMap.class));
    }
    return wikiRawEvents;
  }
}
