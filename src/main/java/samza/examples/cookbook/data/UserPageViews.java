package samza.examples.cookbook.data;


import org.codehaus.jackson.annotate.JsonProperty;


public class UserPageViews {
  private final String userId;
  private final int count;

  public UserPageViews(
      @JsonProperty("userId") String userId,
      @JsonProperty("count") int count) {
    this.userId = userId;
    this.count = count;
  }

  public String getUserId() {
    return userId;
  }

  public int getCount() {
    return count;
  }
}
