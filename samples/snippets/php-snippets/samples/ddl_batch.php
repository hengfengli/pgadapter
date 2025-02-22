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

// [START spanner_ddl_batch]
function ddl_batch(string $host, string $port, string $database): void
{
    $dsn = sprintf("pgsql:host=%s;port=%s;dbname=%s", $host, $port, $database);
    $connection = new PDO($dsn);

    // Executing multiple DDL statements as one batch is
    // more efficient than executing each statement
    // individually.
    $connection->exec("start batch ddl");
    $connection->exec("CREATE TABLE venues ("
        ."  venue_id    bigint not null primary key,"
        ."  name        varchar(1024),"
        ."  description jsonb"
        .")");
    $connection->exec("CREATE TABLE concerts ("
        ."  concert_id bigint not null primary key ,"
        ."  venue_id   bigint not null,"
        ."  singer_id  bigint not null,"
        ."  start_time timestamptz,"
        ."  end_time   timestamptz,"
        ."  constraint fk_concerts_venues foreign key"
        ."    (venue_id) references venues (venue_id),"
        ."  constraint fk_concerts_singers foreign key"
        ."    (singer_id) references singers (singer_id)"
        .")");
    $connection->exec("run batch");
    print("Added venues and concerts tables\n");

    $connection = null;
}
// [END spanner_ddl_batch]
