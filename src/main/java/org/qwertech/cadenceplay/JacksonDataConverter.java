package org.qwertech.cadenceplay;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Defaults;
import com.google.common.collect.Sets;
import com.uber.cadence.ActivityType;
import com.uber.cadence.common.RetryOptions;
import com.uber.cadence.converter.DataConverter;
import com.uber.cadence.converter.DataConverterException;
import com.uber.cadence.converter.JsonDataConverter;
import com.uber.cadence.workflow.ActivityFailureException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * 1. Uses Jackson instead of Gson in JsonDataConverter 2. Uses Gson in single argument calls to work around problems with jackson (client version 2.5.1) a.
 * LocalActivityMarkerHeader in method com.uber.cadence.internal.common.LocalActivityMarkerData#fromEventAttributes(com.uber.cadence.MarkerRecordedEventAttributes,
 * com.uber.cadence.converter.DataConverter)
 */
@Slf4j
public class JacksonDataConverter implements DataConverter {

  private static final ObjectMapper DEFAULT_JACKSON = getDefaultJackson();
  private static final DataConverter INSTANCE = new JacksonDataConverter();
  private static final Set<String> GSON_FALLBACK_CLASSES = Sets.newHashSet(
      "com.uber.cadence.internal.common.LocalActivityMarkerData.LocalActivityMarkerHeader"
  );

  private static final DataConverter GSON_CONVERTER = JsonDataConverter.getInstance();
  public static final String MARKER_HEADER = "com.uber.cadence.internal.replay.MarkerHandler$MarkerData$MarkerHeader";

  private final ObjectMapper jackson;

  private boolean logTestMode;

  @SneakyThrows
  public JacksonDataConverter(ObjectMapper jackson) {
    this.jackson = jackson;
  }

  private JacksonDataConverter() {
    this(DEFAULT_JACKSON);
  }

  public static DataConverter getInstance() {
    return INSTANCE;
  }

  @Override
  public byte[] toData(Object... values) throws DataConverterException {
    if (values == null || values.length == 0) {
      return null;
    }
    try {
      if (values.length == 1) {
        return values[0] == null ? null : toDataSingleValue(values[0]);
      }
      byte[] json = jackson.writeValueAsBytes(values);
      logToDataMultipleValuesResult(json, values);
      return json;
    } catch (Exception e) {
      throw new DataConverterException(e);
    }
  }

  @Override
  public <T> T fromData(byte[] content, Class<T> valueClass, Type valueType) {
    if (content == null || valueClass == null || void.class.isAssignableFrom(valueClass) || Void.class.isAssignableFrom(valueClass)) {
      return null;
    }
    if (needFallbackToGson(valueClass)) {
      return fromDataGson(content, valueClass, valueType);
    }

    try {
      logFromDataCall(content, valueClass, valueType);
      if (valueType != null) {
        JavaType javaType = jackson.constructType(valueType);
        return jackson.readValue(content, javaType);
      }
      return jackson.readValue(content, valueClass);
    } catch (Exception e) {
      throw new DataConverterException(content, new Type[]{valueType}, e);
    }
  }

  @Override
  public Object[] fromDataArray(byte[] content, Type... valueTypes) throws DataConverterException {
    try {
      if (content == null) {
        if (valueTypes.length == 0) {
          return new Object[0];
        }
        throw new DataConverterException("Content doesn't match expected arguments", content, valueTypes);
      }

      if (valueTypes.length == 1) {
        Object result = fromDataArrayBySingleType(content, valueTypes[0]);
        return new Object[]{result};
      }

      logFromDataArrayCall(content, valueTypes);
      final JsonNode element = jackson.readTree(content);
      return getObjectsFromJsonNodeElements(element, valueTypes);
    } catch (Exception e) {
      throw new DataConverterException(content, valueTypes, e);
    }
  }

  private static ObjectMapper getDefaultJackson() {
    return new ObjectMapper()
        .enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES)
        .setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
  }

  private Object[] getObjectsFromJsonNodeElements(JsonNode element, Type... valueTypes) {
    ArrayNode array;
    if (element instanceof ArrayNode) {
      array = (ArrayNode) element;
    } else {
      array = jackson.createArrayNode();
      array.add(element);
    }

    Object[] result = new Object[valueTypes.length];
    for (int i = 0; i < valueTypes.length; i++) {

      if (i >= array.size()) {
        Type t = valueTypes[i];
        if (t instanceof Class) {
          result[i] = Defaults.defaultValue((Class<?>) t);
        } else {
          result[i] = null;
        }
      } else {
        final JavaType javaType = jackson.constructType(valueTypes[i]);
        final JsonNode jsonNode = array.get(i);
        result[i] = jackson.convertValue(jsonNode, javaType);
      }
    }
    return result;
  }

  private Object fromDataArrayBySingleType(byte[] content, Type valueType) throws IOException {
    logFromDataArraySingleTypeCall(content, valueType);
    final JavaType javaType = jackson.constructType(valueType);
    return jackson.readValue(content, javaType);
  }

  private byte[] toDataSingleValue(Object value) throws JsonProcessingException {
    if (value != null && needFallbackToGson(value.getClass())) {
      return toDataGson(value);
    }
    final byte[] json = jackson.writeValueAsBytes(value);
    logToDataSingleValueResult(json, value);
    return json;
  }

  private byte[] toDataGson(Object... values) throws DataConverterException {
    final byte[] bytes = GSON_CONVERTER.toData(values);
    logToDataGsonResult(bytes, values);
    return bytes;
  }

  private <T> T fromDataGson(byte[] content, Class<T> valueClass, Type valueType) {
    logFromDataGsonCall(content, valueClass, valueType);
    return GSON_CONVERTER.fromData(content, valueClass, valueClass);
  }

  private <T> boolean needFallbackToGson(Class<T> valueClass) {
    return GSON_FALLBACK_CLASSES.contains(valueClass.getCanonicalName());
  }

  // LOGGING
  // toData

  private void logToDataGsonResult(byte[] bytes, Object[] values) {
    if (isTraceEnabled()) {
      final List<String> classes = getSimpleClassNames(values);
      log.trace(
          "[toData] fall back to Gson DataConverter, values of classes: {}, result: {}",
          classes, bytesToString(bytes));
    }
  }

  private void logToDataMultipleValuesResult(byte[] json, Object[] values) {
    if (isTraceEnabled()) {
      final List<String> classes = getSimpleClassNames(values);
      log.trace("[toData] operation result for values of classes {}: '{}'", classes, bytesToString(json));
    }
  }

  private void logToDataSingleValueResult(byte[] json, Object value) {
    if (isTraceEnabled()) {
      log.trace(
          "[toData] result for single value of class {}: '{}'",
          Optional.ofNullable(value)
              .map(Object::getClass)
              .map(Class::getCanonicalName)
              .orElse("null value.no class info"), bytesToString(json)
      );
    }
  }

  // fromData

  private <T> void logFromDataGsonCall(byte[] content, Class<T> valueClass, Type valueType) {
    if (isTraceEnabled()) {
      final String contentJson = bytesToString(content);
      log.trace(
          "[fromData] fall back to Gson DataConverter for class: {}[{}], content: {}",
          valueClass, valueType, contentJson
      );
    }
  }

  private <T> void logFromDataCall(byte[] content, Class<T> valueClass, Type valueType) {
    if (isTraceEnabled()) {
      final String contentJson = bytesToString(content);
      log.trace(
          "[fromData] processing class: {}|{}, content: {}",
          valueClass, valueType, contentJson
      );
    }
  }

  // fromDataArray

  private void logFromDataArrayCall(byte[] content, Type[] valueTypes) {
    if (isTraceEnabled()) {
      final String contentJson = bytesToString(content);
      log.trace(
          "[fromDataArray] processing types: {}, content: {}",
          valueTypes, contentJson
      );
    }
  }

  private void logFromDataArraySingleTypeCall(byte[] content, Type valueType) {
    if (isTraceEnabled()) {
      final String contentJson = bytesToString(content);
      log.trace(
          "[fromDataArray] processing single type: {}, content: '{}'",
          valueType, contentJson
      );
    }
  }

  private List<String> getSimpleClassNames(Object[] values) {
    return Arrays
        .stream(values)
        .map(Object::getClass)
        .map(Class::getSimpleName)
        .collect(Collectors.toList());
  }

  private String bytesToString(byte[] content) {
    return new String(content, StandardCharsets.UTF_8);
  }

  protected void setLogTestMode(boolean logTestMode) {
    this.logTestMode = logTestMode;
  }


  private boolean isTraceEnabled() {
    return log.isTraceEnabled() || logTestMode;
  }


}
