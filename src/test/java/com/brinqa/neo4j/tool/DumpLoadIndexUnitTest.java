package com.brinqa.neo4j.tool;

import static com.brinqa.neo4j.tool.util.Print.println;

import com.brinqa.neo4j.tool.dto.IndexData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.List;
import org.junit.Test;

/** Attempt to recreate all the indexes */
public class DumpLoadIndexUnitTest {

  @Test
  public void indexLoad() throws Exception {
    final var mapper = new ObjectMapper();
    final var f = new File("dump-index.json");
    final var typedef = new TypeReference<List<IndexData>>() {};
    final var indexes = mapper.readValue(f, typedef);

    for (final var index : indexes) {
      if (index.getOwningConstraint() != null) {
        println("index: " + index.getOwningConstraint());
      }
    }
  }
}
