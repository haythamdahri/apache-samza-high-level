package samza.examples.wikipedia.application;

import joptsimple.OptionSet;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.util.CommandLine;


public class WikipediaZkLocalApplication {

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config config = cmdLine.loadConfig(options);

    WikipediaApplication app = new WikipediaApplication();
    LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
    runner.run();
    runner.waitForFinish();
  }
}
