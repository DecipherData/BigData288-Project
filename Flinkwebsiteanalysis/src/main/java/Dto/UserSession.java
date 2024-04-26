package Dto;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

@Data
public class UserSession implements Serializable {
    private String sessionId;
    private String userId;
    private String gender;
    private String sessionTimestamp;
    private List<Integer> productIds;
    private List<String> productNames;
    private List<Integer> productPrices;
    private List<String> productCategories;
    private List<String> productReviews;
    private String name;
    private String email;
    private DeviceInformation deviceInformation;
    private LocationData locationData;
    private String ipAddress;
    private int sessionDuration;
    private int pageViews;
    private List<Integer> browsingPattern;
    private List<Integer> timeSpentonEachPage;
    private int clicks;
    private String actions;
    private String referrer;
    private String exitPage;
    private boolean purchaseMade;
    private double amountSpent;
    private String paymentMethodType;

    // Default constructor
    //public UserSession() {}

    // Getters and setters for each field
    // Add here...

    // Remember to override toString() if needed for debugging
    // Add here...

    // Getters and Setters
    // Example:
    //public String getSessionId() {
    //    return sessionId;
    //}

    //public void setSessionId(String sessionId) {
    //    this.sessionId = sessionId;
    //}
}
