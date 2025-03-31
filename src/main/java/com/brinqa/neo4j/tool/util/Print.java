/*
 * Copyright 2025 Brinqa, Inc. All rights reserved.
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
package com.brinqa.neo4j.tool.util;

public class Print {

  public static void println(String fmt, Object... args) {
    System.out.printf(fmt + "%n", args);
  }

  public static void printf(String fmt, Object... args) {
    System.out.printf(fmt, args);
  }

  public static void progressPercentage(int remain) {
    if (remain > 100) {
      throw new IllegalArgumentException();
    }
    progressPercentageBar(remain);
    if (remain == 100) {
      System.out.print("\n");
    }
  }

  public static void progressPercentage(long count, long total) {
    int remain = (int) ((100 * count) / total);
    progressPercentageBar(remain);
    printf(" (%d/%d)", count, total);
    if (remain == 100) {
      System.out.print("\n");
    }
  }

  static void progressPercentageBar(int remain) {
    int maxBareSize = 100; // 100 unit for 100%
    char defaultChar = '-';
    String icon = "*";
    String bare = new String(new char[maxBareSize]).replace('\0', defaultChar) + "]";
    String bareDone = "[" + icon.repeat(Math.max(0, remain));
    String bareRemain = bare.substring(remain);
    System.out.print("\r" + bareDone + bareRemain + " " + remain + "%");
  }
}
