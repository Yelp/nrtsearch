package org.apache.platypus.server;

import java.util.List;
import java.util.Map;

//
//{"unique_id":12471654,
// "score":38,
// "id":12471654,
// "localized_completed_text":"T & M Market",
// "location":{"x":-122.4225765,"y":37.7679796},
// "country":"US",
// "review_count":2,
// "review_wilson_score":0.21913035879807372,
// "category_aliases":["food","grocery"],
// "checkin_rate_per_day":0.0,
// "standardized_score":38,
// "language_alternate_names":[]}
//
public class BusinessSuggestionRecord {

    private Long unique_id;
    private Long score;
    private Long id;
    private String localized_completed_text;
    private Map<String, Double> location;
    private String country;
    private Integer review_count;
    private Double review_wilson_score;
    private List<String> category_aliases;
    private Double checkin_rate_per_day;
    private Long standardized_score;
    private List<Map<String, String>> language_alternate_names;

    public Long getUnique_id() {
        return unique_id;
    }

    public Long getScore() {
        return score;
    }

    public Long getId() {
        return id;
    }

    public String getLocalized_completed_text() {
        return localized_completed_text;
    }

    public Map<String, Double> getLocation() {
        return location;
    }

    public Double getLocationLat(){
        return getLocation().get("y");
    }

    public Double getLocationLon(){
        return getLocation().get("x");
    }

    public String getCountry() {
        return country;
    }

    public Integer getReview_count() {
        return review_count;
    }

    public Double getReview_wilson_score() {
        return review_wilson_score;
    }

    public List<String> getCategory_aliases() {
        return category_aliases;
    }

    public Double getCheckin_rate_per_day() {
        return checkin_rate_per_day;
    }

    public Long getStandardized_score() {
        return standardized_score;
    }

    public List<Map<String, String>> getLanguage_alternate_names() {
        return language_alternate_names;
    }
}
