package utils;

import Dto.UserSession;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

public class JsonUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String convertSessionToJson(UserSession usersession) {
        try {
            Map<String , String> userMap = new HashMap<>();
            userMap.put("sessionid" , usersession.getSessionId());
            userMap.put("name" , usersession.getName() );
            userMap.put("userid" , usersession.getUserId());
            return objectMapper.writeValueAsString(userMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }
}