/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.brinqa.neo4j.tool.data;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * Turns {@link NodeRecord}/{@link RelRecord} into bytes with Kryo. Property values are kept in
 * their normalized form (see {@link PropertyCodec}) so Kryo only ever sees plain JDK types and
 * never has to reflect into a closed java.base module.
 *
 * <p>Kryo is not thread safe; each codec instance owns one Kryo and must be used single threaded.
 */
final class KryoCodec {

  private final Kryo kryo = new Kryo();
  private final Output output = new Output(4096, -1);

  KryoCodec() {
    kryo.setRegistrationRequired(false);
    kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
    kryo.register(NodeRecord.class);
    kryo.register(RelRecord.class);
    kryo.register(ArrayList.class);
    kryo.register(LinkedHashMap.class);
  }

  byte[] encodeNode(NodeRecord record) {
    output.reset();
    kryo.writeObject(output, record);
    return output.toBytes();
  }

  NodeRecord decodeNode(byte[] bytes) {
    try (var in = new Input(bytes)) {
      return kryo.readObject(in, NodeRecord.class);
    }
  }

  byte[] encodeRel(RelRecord record) {
    output.reset();
    kryo.writeObject(output, record);
    return output.toBytes();
  }

  RelRecord decodeRel(byte[] bytes) {
    try (var in = new Input(bytes)) {
      return kryo.readObject(in, RelRecord.class);
    }
  }
}
