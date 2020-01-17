
package samza.examples.azure;

import joptsimple.OptionSet;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.util.CommandLine;


public class AzureZKLocalApplication {

  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config config = cmdLine.loadConfig(options);

    AzureApplication app = new AzureApplication();
    LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
    runner.run();

    runner.waitForFinish();
  }

}
