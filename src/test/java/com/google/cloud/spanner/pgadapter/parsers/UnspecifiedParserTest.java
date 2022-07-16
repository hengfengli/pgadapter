// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spanner.pgadapter.parsers;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.parsers.Parser.FormatCode;
import com.google.protobuf.NullValue;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.core.Oid;

@RunWith(JUnit4.class)
public class UnspecifiedParserTest {

  @Test
  public void testCreateUnspecifiedParser() {
    Parser<?> parser = Parser.create(new byte[] {}, Oid.UNSPECIFIED, FormatCode.TEXT);

    assertEquals(UnspecifiedParser.class, parser.getClass());
  }

  @Test
  public void testParseNonNull() {
    Parser<?> parser =
        Parser.create(
            "unspecified value".getBytes(StandardCharsets.UTF_8), Oid.UNSPECIFIED, FormatCode.TEXT);

    assertEquals(
        Value.untyped(
            com.google.protobuf.Value.newBuilder().setStringValue("unspecified value").build()),
        parser.getItem());
    assertEquals("unspecified value", parser.stringParse());
    assertArrayEquals("unspecified value".getBytes(StandardCharsets.UTF_8), parser.binaryParse());
  }

  @Test
  public void testStringParse() {
    assertEquals(
        "test",
        new UnspecifiedParser(
                Value.untyped(
                    com.google.protobuf.Value.newBuilder().setStringValue("test").build()))
            .stringParse());
    assertNull(
        new UnspecifiedParser(
                Value.untyped(
                    com.google.protobuf.Value.newBuilder()
                        .setNullValue(NullValue.NULL_VALUE)
                        .build()))
            .stringParse());
    assertNull(new UnspecifiedParser(null).stringParse());
  }
}
