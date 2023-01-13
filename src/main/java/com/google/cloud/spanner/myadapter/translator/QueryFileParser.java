package com.google.cloud.spanner.myadapter.translator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class QueryFileParser {

  private static final Gson GSON = new Gson();

  private static final String DEFAULT_FILE = "metadata/default_command_metadata.json";
  private static final String EMPTY_FILE = "metadata/empty_command_metadata.json";


  @VisibleForTesting
  public List<TranslatedQuery> defaultCommands() throws IOException, ParseException {
    return parse(this.getClass().getClassLoader().getResourceAsStream(DEFAULT_FILE));
  }

  private static List<TranslatedQuery> parse(InputStream inputStream)
      throws JsonIOException, JsonSyntaxException, IOException {
    try (InputStreamReader reader = new InputStreamReader(inputStream)) {
      return GSON.fromJson(new JsonReader(reader), new TypeToken<List<TranslatedQuery>>() {
      }.getType());
    }
  }

}
