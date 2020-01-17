package samza.examples.test.utils;

import com.google.common.io.Resources;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.codehaus.jackson.map.ObjectMapper;
import samza.examples.cookbook.data.PageView;
import samza.examples.wikipedia.application.WikipediaApplication;

import static samza.examples.wikipedia.system.WikipediaFeed.WikipediaFeedEvent;


public class TestUtils {

  public static List<WikipediaFeedEvent> genWikipediaFeedEvents(String channel) {
    List<String> wikiEvents = null;
    switch (channel) {
      case WikipediaApplication.WIKIPEDIA_CHANNEL:
        wikiEvents = readFile("WikipediaEditEvents.txt");
        break;

      case WikipediaApplication.WIKINEWS_CHANNEL:
        wikiEvents = readFile("WikinewsEditEvents.txt");
        break;

      case WikipediaApplication.WIKTIONARY_CHANNEL:
        wikiEvents = readFile("WikitionaryEditEvents.txt");
        break;
    }
    ObjectMapper mapper = new ObjectMapper();
    return wikiEvents.stream().map(event -> {
      try {
        return new WikipediaFeedEvent(mapper.readValue(event, HashMap.class));
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }).filter(x -> x != null).collect(Collectors.toList());
  }

  public static List<PageView> genSamplePageViewData() {
    List<PageView> pageViewEvents = new ArrayList<>();
    pageViewEvents.add(new PageView("google.com/home", "user1", "india"));
    pageViewEvents.add(new PageView("google.com/search", "user1", "india"));
    pageViewEvents.add(new PageView("yahoo.com/home", "user2", "china"));
    pageViewEvents.add(new PageView("yahoo.com/search", "user2", "china"));
    pageViewEvents.add(new PageView("google.com/news", "user1", "india"));
    pageViewEvents.add(new PageView("yahoo.com/fashion", "user2", "china"));
    return pageViewEvents;
  }

  private static List<String> readFile(String path) {
    try {
      InputStream in = Resources.getResource(path).openStream();
      List<String> lines = new ArrayList<>();
      String line = null;
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
      reader.close();
      return lines;
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
