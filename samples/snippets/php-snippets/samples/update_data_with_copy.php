<?php

// Copyright 2025 Google LLC
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

// [START spanner_update_data]
function update_data_with_copy(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);

    // Instruct PGAdapter to use insert-or-update for COPY statements.
    // This enables us to use COPY to update data.
    $connection->exec("set spanner.copy_upsert=true");

    // COPY uses mutations to insert or update existing data in Spanner.
    $connection->pgsqlCopyFromArray(
        "albums",
        ["1\t1\t100000", "2\t2\t500000"],
        "\t",
        "\\\\N",
        "singer_id, album_id, marketing_budget",
    );
    print("Updated 2 albums\n");

    $connection = null;
}
// [END spanner_update_data]
