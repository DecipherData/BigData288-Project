package Deserializer;

import Dto.UserSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JSONValueDeserializationSchema implements DeserializationSchema<UserSession> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public UserSession deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, UserSession.class);
    }

    @Override
    public boolean isEndOfStream(UserSession userSession) {
        return false;
    }

    @Override
    public TypeInformation<UserSession> getProducedType() {
        return TypeInformation.of(UserSession.class);
    }
}
