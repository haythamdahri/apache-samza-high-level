
package samza.examples.cookbook.data;

import org.codehaus.jackson.annotate.JsonProperty;

public class AdClick {

  private String pageId; 
  private String adId; 
  private String userId; 

  public AdClick(
      @JsonProperty("pageId") String pageId,
      @JsonProperty("adId") String adId,
      @JsonProperty("userId") String userId) {
    this.pageId = pageId;
    this.adId = adId;
    this.userId = userId;
  }

  public String getPageId() {
    return pageId;
  }

  public void setPageId(String pageId) {
    this.pageId = pageId;
  }

  public String getAdId() {
    return adId;
  }

  public void setAdId(String adId) {
    this.adId = adId;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }
}