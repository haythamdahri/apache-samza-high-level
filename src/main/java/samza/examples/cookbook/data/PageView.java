package samza.examples.cookbook.data;

import org.codehaus.jackson.annotate.JsonProperty;


public class PageView {
  public final String userId;
  public final String country;
  public final String pageId;

  public PageView(
      @JsonProperty("pageId") String pageId,
      @JsonProperty("userId") String userId,
      @JsonProperty("countryId") String country) {
    this.userId = userId;
    this.country = country;
    this.pageId = pageId;
  }
}
