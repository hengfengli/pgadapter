// Copyright 2023 Google LLC
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

package com.google.cloud.spanner.pgadapter.nodejs;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.pgadapter.AbstractMockServerTest;
import java.io.File;
import java.io.IOException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PrismaSampleTest extends AbstractMockServerTest {
  static File getTestDirectory() throws IOException {
    String currentPath = new java.io.File(".").getCanonicalPath();
    String testDirectoryPath = String.format("%s/samples/nodejs/prisma/src", currentPath);
    return new File(testDirectoryPath);
  }

  @BeforeClass
  public static void installDependencies() throws IOException, InterruptedException {
    NodeJSTest.installDependencies(getTestDirectory());
  }

  static String runTest(String testName) throws IOException, InterruptedException {
    return NodeJSTest.runTest(
        getTestDirectory(), "test", testName, "localhost", pgServer.getLocalPort(), "db");
  }

  @Test
  public void testCreateDataModel() throws Exception {
    addDdlResponseToSpannerAdmin();
    addDdlResponseToSpannerAdmin();

    String output = runTest("testCreateDataModel");

    assertEquals(
        "Creating sample data model...\n"
            + "Sample data model created.\n"
            + "Successfully executed sample application\n",
        output);
  }
}
