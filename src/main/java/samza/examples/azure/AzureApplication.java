
package samza.examples.azure;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.eventhub.descriptors.EventHubsInputDescriptor;
import org.apache.samza.system.eventhub.descriptors.EventHubsOutputDescriptor;
import org.apache.samza.system.eventhub.descriptors.EventHubsSystemDescriptor;


public class AzureApplication implements StreamApplication {
  private static final String INPUT_STREAM_ID = "input-stream";
  private static final String OUTPUT_STREAM_ID = "output-stream";

  private static final String EVENTHUBS_NAMESPACE = "my-eventhubs-namespace";

  private static final String EVENTHUBS_INPUT_ENTITY = "my-input-entity";
  private static final String EVENTHUBS_OUTPUT_ENTITY = "my-output-entity";

  private static final String EVENTHUBS_SAS_KEY_NAME_CONFIG = "sensitive.eventhubs.sas.key.name";
  private static final String EVENTHUBS_SAS_KEY_TOKEN_CONFIG = "sensitive.eventhubs.sas.token";

  @Override
  public void describe(StreamApplicationDescriptor appDescriptor) {
    EventHubsSystemDescriptor systemDescriptor = new EventHubsSystemDescriptor("eventhubs");

    StringSerde serde = new StringSerde();

    EventHubsInputDescriptor<KV<String, String>> inputDescriptor =
        systemDescriptor.getInputDescriptor(INPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_INPUT_ENTITY, serde)
            .withSasKeyName(appDescriptor.getConfig().get(EVENTHUBS_SAS_KEY_NAME_CONFIG))
            .withSasKey(appDescriptor.getConfig().get(EVENTHUBS_SAS_KEY_TOKEN_CONFIG));

    EventHubsOutputDescriptor<KV<String, String>> outputDescriptor =
        systemDescriptor.getOutputDescriptor(OUTPUT_STREAM_ID, EVENTHUBS_NAMESPACE, EVENTHUBS_OUTPUT_ENTITY, serde)
            .withSasKeyName(appDescriptor.getConfig().get(EVENTHUBS_SAS_KEY_NAME_CONFIG))
            .withSasKey(appDescriptor.getConfig().get(EVENTHUBS_SAS_KEY_TOKEN_CONFIG));

    MessageStream<KV<String, String>> eventhubInput = appDescriptor.getInputStream(inputDescriptor);
    OutputStream<KV<String, String>> eventhubOutput = appDescriptor.getOutputStream(outputDescriptor);

    eventhubInput
        .map((message) -> {
          System.out.println("Sending: ");
          System.out.println("Received Key: " + message.getKey());
          System.out.println("Received Message: " + message.getValue());
          return message;
        })
        .sendTo(eventhubOutput);
  }
}
