
package samza.examples.cookbook.data;

import org.codehaus.jackson.annotate.JsonProperty;


public class Profile {

  public final String userId;
  public final String company;

  public Profile(
      @JsonProperty("userId") String userId,
      @JsonProperty("company") String company) {
    this.userId = userId;
    this.company = company;
  }

}
