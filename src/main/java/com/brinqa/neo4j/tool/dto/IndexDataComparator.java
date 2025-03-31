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

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/** Sort by constraints first then indexes. */
public class IndexDataComparator implements Comparator<IndexData> {
    private final StringListComparator stringListComparator = new StringListComparator();

    @Override
    public int compare(IndexData o1, IndexData o2) {
        // constraints first
        int cmp = o1.getName().compareTo(o2.getName());
        if (0 != cmp) {
            return cmp;
        }
        // type
        cmp = o1.getType().compareTo(o2.getType());
        if (0 != cmp) {
            return cmp;
        }
        // compare labels
        return stringListComparator.compare(o1.getLabelsOrTypes(), o2.getLabelsOrTypes());
    }

    /** Just sort based on number of items in list first (more complex) to least complex. */
    static class StringListComparator implements Comparator<List<String>> {

        @Override
        public int compare(List<String> l1, List<String> l2) {
            int cmp = Integer.compare(l1.size(), l2.size());
            if (0 != cmp) {
                return cmp;
            }

            final List<String> s1 = l1.stream().sorted().collect(Collectors.toList());
            final List<String> s2 = l2.stream().sorted().collect(Collectors.toList());
            for (int i = 0; i < s1.size(); i++) {
                final String sc1 = s1.get(i);
                cmp = sc1.compareTo(s2.get(i));
                if (0 != cmp) {
                    return cmp;
                }
            }
            return 0;
        }
    }
}
