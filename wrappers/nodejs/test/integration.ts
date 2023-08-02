/*!
 * Copyright 2023 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {describe, it} from "mocha";
import {ExecutionEnvironment, PGAdapter} from "../src";
import {Client} from "pg";
import assert = require("assert");

describe('PGAdapter', () => {
  describe('connect', () => {
    it('should connect to Cloud Spanner using Java', async () => {
      const pg = new PGAdapter({
        executionEnvironment: ExecutionEnvironment.Java,
        project: "appdev-soda-spanner-staging",
        instance: "knut-test-ycsb",
        credentialsFile: "/Users/loite/Downloads/appdev-soda-spanner-staging.json",
      });
      await runConnectionTest(pg);
    });

    it('should connect to Cloud Spanner using Docker', async () => {
      const pg = new PGAdapter({
        executionEnvironment: ExecutionEnvironment.Docker,
        project: "appdev-soda-spanner-staging",
        instance: "knut-test-ycsb",
        credentialsFile: "/Users/loite/Downloads/appdev-soda-spanner-staging.json",
      });
      await runConnectionTest(pg);
    });
  });
});


async function runConnectionTest(pg: PGAdapter) {
  await pg.start();
  const port = pg.getHostPort();

  const client = new Client({host: "localhost", port: port, database: "knut-test-db"});
  await client.connect();

  const res = await client.query('SELECT $1::text as message', ['Hello world!'])
  assert.strictEqual(res.rows[0].message, "Hello world!");
  await client.end()
  await pg.stop();
}
