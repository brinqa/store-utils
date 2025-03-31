/*
 * Copyright 2022 Brinqa, Inc. All rights reserved.
 *
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
package com.brinqa.neo4j.tool.dto;

import java.util.List;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Jacksonized
@Builder(toBuilder = true)
public class IndexData {
    public enum Type {
        RANGE,
        FULLTEXT,
        TEXT,
        LOOKUP
    }

    long id;
    String name;
    String state;
    float populationPercent;
    Type type;
    String entityType;
    List<String> labelsOrTypes;
    List<String> properties;
    String indexProvider;
    String owningConstraint;
    long readCount;
}
