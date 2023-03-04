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

package com.google.cloud.spanner.pgadapter.statements;

import com.google.api.core.InternalApi;
import com.google.cloud.Tuple;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.pgadapter.session.SessionState;
import com.google.cloud.spanner.pgadapter.statements.SimpleParser.TableOrIndexName;
import com.google.cloud.spanner.pgadapter.utils.ClientAutoDetector.WellKnownClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

@InternalApi
public class PgCatalog {
  private static final ImmutableMap<TableOrIndexName, TableOrIndexName> DEFAULT_TABLE_REPLACEMENTS =
      ImmutableMap.<TableOrIndexName, TableOrIndexName>builder()
          .put(
              new TableOrIndexName("pg_catalog", "pg_namespace"),
              new TableOrIndexName(null, "pg_namespace"))
          .put(
              new TableOrIndexName(null, "pg_namespace"),
              new TableOrIndexName(null, "pg_namespace"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_class"),
              new TableOrIndexName(null, "pg_class"))
          .put(new TableOrIndexName(null, "pg_class"), new TableOrIndexName(null, "pg_class"))
          .put(new TableOrIndexName("pg_catalog", "pg_proc"), new TableOrIndexName(null, "pg_proc"))
          .put(new TableOrIndexName(null, "pg_proc"), new TableOrIndexName(null, "pg_proc"))
          .put(new TableOrIndexName("pg_catalog", "pg_enum"), new TableOrIndexName(null, "pg_enum"))
          .put(new TableOrIndexName(null, "pg_enum"), new TableOrIndexName(null, "pg_enum"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_range"),
              new TableOrIndexName(null, "pg_range"))
          .put(new TableOrIndexName(null, "pg_range"), new TableOrIndexName(null, "pg_range"))
          .put(new TableOrIndexName("pg_catalog", "pg_type"), new TableOrIndexName(null, "pg_type"))
          .put(new TableOrIndexName(null, "pg_type"), new TableOrIndexName(null, "pg_type"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_settings"),
              new TableOrIndexName(null, "pg_settings"))
          .put(new TableOrIndexName(null, "pg_settings"), new TableOrIndexName(null, "pg_settings"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_constraint"),
              new TableOrIndexName(null, "pg_constraint"))
          .put(
              new TableOrIndexName(null, "pg_constraint"),
              new TableOrIndexName(null, "pg_constraint"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_attribute"),
              new TableOrIndexName(null, "pg_attribute"))
          .put(
              new TableOrIndexName(null, "pg_attribute"),
              new TableOrIndexName(null, "pg_attribute"))
          .put(
              new TableOrIndexName("pg_catalog", "pg_attrdef"),
              new TableOrIndexName(null, "pg_attrdef"))
          .put(new TableOrIndexName(null, "pg_attrdef"), new TableOrIndexName(null, "pg_attrdef"))
          .build();

  private static final ImmutableMap<Pattern, Supplier<String>> DEFAULT_FUNCTION_REPLACEMENTS =
      ImmutableMap.of(
          Pattern.compile("pg_catalog.pg_table_is_visible\\s*\\(.+\\)"),
              Suppliers.ofInstance("true"),
          Pattern.compile("pg_table_is_visible\\s*\\(.+\\)"), Suppliers.ofInstance("true"),
          Pattern.compile("=\\s*ANY\\s*\\(current_schemas\\(true\\)\\)"),
              Suppliers.ofInstance(" IN ('pg_catalog', 'public')"));

  private final ImmutableSet<String> checkPrefixes;

  private final ImmutableMap<TableOrIndexName, TableOrIndexName> tableReplacements;
  private final ImmutableMap<TableOrIndexName, PgCatalogTable> pgCatalogTables;

  private final ImmutableMap<Pattern, Supplier<String>> functionReplacements;

  private static final Map<TableOrIndexName, PgCatalogTable> DEFAULT_PG_CATALOG_TABLES =
      ImmutableMap.of(
          new TableOrIndexName(null, "pg_namespace"), new PgNamespace(),
          new TableOrIndexName(null, "pg_class"), new PgClass(),
          new TableOrIndexName(null, "pg_proc"), new PgProc(),
          new TableOrIndexName(null, "pg_enum"), new EmptyPgEnum(),
          new TableOrIndexName(null, "pg_range"), new PgRange(),
          new TableOrIndexName(null, "pg_type"), new PgType(),
          new TableOrIndexName(null, "pg_constraint"), new EmptyPgConstraint(),
          new TableOrIndexName(null, "pg_attribute"), new EmptyPgAttribute(),
          new TableOrIndexName(null, "pg_attrdef"), new EmptyPgAttrdef());
  private final SessionState sessionState;

  public PgCatalog(@Nonnull SessionState sessionState, @Nonnull WellKnownClient wellKnownClient) {
    this.sessionState = Preconditions.checkNotNull(sessionState);
    this.checkPrefixes = wellKnownClient.getPgCatalogCheckPrefixes();
    ImmutableMap.Builder<TableOrIndexName, TableOrIndexName> builder =
        ImmutableMap.<TableOrIndexName, TableOrIndexName>builder()
            .putAll(DEFAULT_TABLE_REPLACEMENTS);
    wellKnownClient
        .getTableReplacements()
        .forEach((k, v) -> builder.put(TableOrIndexName.parse(k), TableOrIndexName.parse(v)));
    this.tableReplacements = builder.build();

    ImmutableMap.Builder<TableOrIndexName, PgCatalogTable> pgCatalogTablesBuilder =
        ImmutableMap.<TableOrIndexName, PgCatalogTable>builder()
            .putAll(DEFAULT_PG_CATALOG_TABLES)
            .put(new TableOrIndexName(null, "pg_settings"), new PgSettings());
    wellKnownClient
        .getPgCatalogTables()
        .forEach((k, v) -> pgCatalogTablesBuilder.put(TableOrIndexName.parse(k), v));
    this.pgCatalogTables = pgCatalogTablesBuilder.build();

    this.functionReplacements =
        ImmutableMap.<Pattern, Supplier<String>>builder()
            .putAll(DEFAULT_FUNCTION_REPLACEMENTS)
            .put(
                Pattern.compile("version\\(\\)"), () -> "'" + sessionState.getServerVersion() + "'")
            .putAll(wellKnownClient.getFunctionReplacements())
            .build();
  }

  /** Replace supported pg_catalog tables with Common Table Expressions. */
  public Statement replacePgCatalogTables(Statement statement) {
    // Only replace tables if the statement contains at least one of the known prefixes.
    if (checkPrefixes.stream().noneMatch(prefix -> statement.getSql().contains(prefix))) {
      return statement;
    }

    Tuple<Set<TableOrIndexName>, Statement> replacedTablesStatement =
        new TableParser(statement).detectAndReplaceTables(tableReplacements);
    if (replacedTablesStatement.x().isEmpty()) {
      return replacedTablesStatement.y();
    }
    // Add Common Table Expressions for the pg_catalog tables that were detected and replaced in the
    // statement.
    Set<TableOrIndexName> alreadyAdded = new HashSet<>();
    ImmutableList.Builder<String> cteBuilder = ImmutableList.builder();
    for (TableOrIndexName table : replacedTablesStatement.x()) {
      addPgCatalogTable(table, getPgCatalogTable(table), cteBuilder, alreadyAdded);
    }

    return addCommonTableExpressions(replacedTablesStatement.y(), cteBuilder.build());
  }

  String replaceKnownUnsupportedFunctions(Statement statement) {
    String sql = statement.getSql();
    for (Entry<Pattern, Supplier<String>> functionReplacement : functionReplacements.entrySet()) {
      sql =
          functionReplacement
              .getKey()
              .matcher(sql)
              .replaceAll(functionReplacement.getValue().get());
    }
    return sql;
  }

  Statement addCommonTableExpressions(Statement statement, ImmutableList<String> tableExpressions) {
    if (tableExpressions.isEmpty()) {
      return statement;
    }

    String sql = replaceKnownUnsupportedFunctions(statement);
    SimpleParser parser = new SimpleParser(sql);
    boolean hadCommonTableExpressions = parser.eatKeyword("with");
    String tableExpressionsSql = String.join(",\n", tableExpressions);
    Statement.Builder builder =
        Statement.newBuilder("with ")
            .append(tableExpressionsSql)
            .append(hadCommonTableExpressions ? ",\n" : "\n");
    if (hadCommonTableExpressions) {
      // Include the entire original statement except the 'with' keyword.
      builder
          .append(parser.getSql().substring(0, parser.getPos() - 4))
          .append(parser.getSql().substring(parser.getPos()));
    } else {
      // Include the entire original statement (including any comments at the beginning).
      builder.append(parser.getSql());
    }
    Map<String, Value> parameters = statement.getParameters();
    for (Entry<String, Value> param : parameters.entrySet()) {
      builder.bind(param.getKey()).to(param.getValue());
    }
    statement = builder.build();
    return statement;
  }

  void addPgCatalogTable(
      TableOrIndexName tableName,
      PgCatalogTable pgCatalogTable,
      ImmutableList.Builder<String> cteBuilder,
      Set<TableOrIndexName> alreadyAdded) {
    if (pgCatalogTable == null) {
      return;
    }
    for (TableOrIndexName dependency : pgCatalogTable.getDependencies()) {
      addPgCatalogTable(dependency, getPgCatalogTable(dependency), cteBuilder, alreadyAdded);
    }
    if (alreadyAdded.add(tableName)) {
      cteBuilder.add(pgCatalogTable.getTableExpression());
    }
  }

  PgCatalogTable getPgCatalogTable(TableOrIndexName tableOrIndexName) {
    if (pgCatalogTables.containsKey(tableOrIndexName)) {
      return pgCatalogTables.get(tableOrIndexName);
    }
    return null;
  }

  @InternalApi
  public interface PgCatalogTable {
    String getTableExpression();

    default ImmutableSet<TableOrIndexName> getDependencies() {
      return ImmutableSet.of();
    }
  }

  private static class PgNamespace implements PgCatalogTable {
    private static final String PG_NAMESPACE_CTE =
        "pg_namespace as (\n"
            + "  select case schema_name when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as oid,\n"
            + "        schema_name as nspname, null as nspowner, null as nspacl\n"
            + "  from information_schema.schemata\n"
            + ")";

    @Override
    public String getTableExpression() {
      return PG_NAMESPACE_CTE;
    }
  }

  private static class PgType implements PgCatalogTable {
    private static final ImmutableSet<TableOrIndexName> DEPENDENCIES =
        ImmutableSet.of(new TableOrIndexName(null, "pg_namespace"));
    private static final String GENERATION_SQL =
        "select 'select '\n"
            + "  || oid || ' as oid, '\n"
            + "  || '''' || typname || ''' as typname, '\n"
            + "  || '(select oid from pg_namespace where nspname=''pg_catalog'') as typnamespace, '\n"
            + "  || 'null as typowner, '\n"
            + "  || typlen || ' as typlen, '\n"
            + "  || typbyval::text || ' as typbyval, '\n"
            + "  || '''' || typtype || ''' as typtype, '\n"
            + "  || '''' || typcategory || ''' as typcategory, '\n"
            + "  || typispreferred::text || ' as typispreferred, '\n"
            + "  || (case typname\n"
            + "        when 'int2' then 'false'\n"
            + "        when 'int4' then 'false'\n"
            + "        when 'float4' then 'false'\n"
            + "        when 'timestamp' then 'false'\n"
            + "        else 'true'\n"
            + "      end) || ' as typisdefined, '\n"
            + "  || '''' || typdelim || ''' as typdelim, '\n"
            + "  || typrelid || ' as typrelid, '\n"
            + "  || typelem || ' as typelem, '\n"
            + "  || typarray || ' as typarray, '\n"
            + "  || '''' || typinput || ''' as typinput, '\n"
            + "  || '''' || typoutput || ''' as typoutput, '\n"
            + "  || '''' || typreceive || ''' as typreceive, '\n"
            + "  || '''' || typsend || ''' as typsend, '\n"
            + "  || '''' || typmodin || ''' as typmodin, '\n"
            + "  || '''' || typmodout || ''' as typmodout, '\n"
            + "  || '''' || typanalyze || ''' as typanalyze, '\n"
            + "  || '''' || typalign || ''' as typalign, '\n"
            + "  || '''' || typstorage || ''' as typstorage, '\n"
            + "  || typnotnull::text || ' as typnotnull, '\n"
            + "  || typbasetype || ' as typbasetype, '\n"
            + "  || typtypmod || ' as typtypmod, '\n"
            + "  || coalesce(typndims || ' as typndims, ', 'null as typndims, ')\n"
            + "  || coalesce(typcollation || ' as typcollation, ', 'null as typcollation, ')\n"
            + "  || coalesce('''' || typdefaultbin || ''' as typdefaultbin, ', 'null as typdefaultbin, ')\n"
            + "  || coalesce('''' || typdefault || ''' as typdefault, ', 'null as typdefault, ')\n"
            + "  || 'null as typacl '\n"
            + "  || 'union all'\n"
            + "from pg_type\n"
            + "where typname in ('bool', 'bytea', 'int2', 'int4', 'int8', 'float4', 'float8',\n"
            + "                  'numeric', 'varchar', 'text', 'jsonb', 'timestamp', 'timestamptz', 'date')\n"
            + ";\n";
    private static final String PG_TYPE_CTE =
        "pg_type as (\n"
            + "  select 16 as oid, 'bool' as typname, 11 as typnamespace, null as typowner, 1 as typlen, true as typbyval, 'b' as typtype, 'B' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1000 as typarray, 'boolin' as typinput, 'boolout' as typoutput, 'boolrecv' as typreceive, 'boolsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'c' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 17 as oid, 'bytea' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1001 as typarray, 'byteain' as typinput, 'byteaout' as typoutput, 'bytearecv' as typreceive, 'byteasend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 20 as oid, 'int8' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1016 as typarray, 'int8in' as typinput, 'int8out' as typoutput, 'int8recv' as typreceive, 'int8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 21 as oid, 'int2' as typname, 11 as typnamespace, null as typowner, 2 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1005 as typarray, 'int2in' as typinput, 'int2out' as typoutput, 'int2recv' as typreceive, 'int2send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 's' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 23 as oid, 'int4' as typname, 11 as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1007 as typarray, 'int4in' as typinput, 'int4out' as typoutput, 'int4recv' as typreceive, 'int4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 25 as oid, 'text' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1009 as typarray, 'textin' as typinput, 'textout' as typoutput, 'textrecv' as typreceive, 'textsend' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 700 as oid, 'float4' as typname, 11 as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1021 as typarray, 'float4in' as typinput, 'float4out' as typoutput, 'float4recv' as typreceive, 'float4send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 701 as oid, 'float8' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'N' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1022 as typarray, 'float8in' as typinput, 'float8out' as typoutput, 'float8recv' as typreceive, 'float8send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1043 as oid, 'varchar' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'S' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1015 as typarray, 'varcharin' as typinput, 'varcharout' as typoutput, 'varcharrecv' as typreceive, 'varcharsend' as typsend, 'varchartypmodin' as typmodin, 'varchartypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1082 as oid, 'date' as typname, 11 as typnamespace, null as typowner, 4 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1182 as typarray, 'date_in' as typinput, 'date_out' as typoutput, 'date_recv' as typreceive, 'date_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1114 as oid, 'timestamp' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1115 as typarray, 'timestamp_in' as typinput, 'timestamp_out' as typoutput, 'timestamp_recv' as typreceive, 'timestamp_send' as typsend, 'timestamptypmodin' as typmodin, 'timestamptypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1184 as oid, 'timestamptz' as typname, 11 as typnamespace, null as typowner, 8 as typlen, true as typbyval, 'b' as typtype, 'D' as typcategory, true as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1185 as typarray, 'timestamptz_in' as typinput, 'timestamptz_out' as typoutput, 'timestamptz_recv' as typreceive, 'timestamptz_send' as typsend, 'timestamptztypmodin' as typmodin, 'timestamptztypmodout' as typmodout, '-' as typanalyze, 'd' as typalign, 'p' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1700 as oid, 'numeric' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'N' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 1231 as typarray, 'numeric_in' as typinput, 'numeric_out' as typoutput, 'numeric_recv' as typreceive, 'numeric_send' as typsend, 'numerictypmodin' as typmodin, 'numerictypmodout' as typmodout, '-' as typanalyze, 'i' as typalign, 'm' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 3802 as oid, 'jsonb' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'U' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 0 as typelem, 3807 as typarray, 'jsonb_in' as typinput, 'jsonb_out' as typoutput, 'jsonb_recv' as typreceive, 'jsonb_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1000 as oid, '_bool' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 16 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1001 as oid, '_bytea' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 17 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1016 as oid, '_int8' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 20 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1005 as oid, '_int2' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 21 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1007 as oid, '_int4' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 23 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1009 as oid, '_text' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 25 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1021 as oid, '_float4' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 700 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1022 as oid, '_float8' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 701 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1015 as oid, '_varchar' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 1043 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 100 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1182 as oid, '_date' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 1082 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1115 as oid, '_timestamp' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, false as typisdefined, ',' as typdelim, 0 as typrelid, 1114 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1185 as oid, '_timestamptz' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 1184 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 1231 as oid, '_numeric' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 1700 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl union all\n"
            + "  select 3807 as oid, '_jsonb' as typname, 11 as typnamespace, null as typowner, -1 as typlen, false as typbyval, 'b' as typtype, 'A' as typcategory, false as typispreferred, true as typisdefined, ',' as typdelim, 0 as typrelid, 3802 as typelem, 0 as typarray, 'array_in' as typinput, 'array_out' as typoutput, 'array_recv' as typreceive, 'array_send' as typsend, '-' as typmodin, '-' as typmodout, '-' as typanalyze, 'i' as typalign, 'x' as typstorage, false as typnotnull, 0 as typbasetype, -1 as typtypmod, 0 as typndims, 0 as typcollation, null as typdefaultbin, null as typdefault, null as typacl\n"
            + ")";

    @Override
    public String getTableExpression() {
      return PG_TYPE_CTE;
    }

    @Override
    public ImmutableSet<TableOrIndexName> getDependencies() {
      return DEPENDENCIES;
    }
  }

  private class PgSettings implements PgCatalogTable {

    @Override
    public String getTableExpression() {
      return sessionState.generatePGSettingsCte();
    }
  }

  private static class PgClass implements PgCatalogTable {
    private static final String PG_CLASS_CTE =
        "pg_class as (\n"
            + "  select\n"
            + "  -1 as oid,\n"
            + "  table_name as relname,\n"
            + "  case table_schema when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as relnamespace,\n"
            + "  0 as reltype,\n"
            + "  0 as reloftype,\n"
            + "  0 as relowner,\n"
            + "  1 as relam,\n"
            + "  0 as relfilenode,\n"
            + "  0 as reltablespace,\n"
            + "  0 as relpages,\n"
            + "  0.0::float8 as reltuples,\n"
            + "  0 as relallvisible,\n"
            + "  0 as reltoastrelid,\n"
            + "  false as relhasindex,\n"
            + "  false as relisshared,\n"
            + "  'p' as relpersistence,\n"
            + "  'r' as relkind,\n"
            + "  count(*) as relnatts,\n"
            + "  0 as relchecks,\n"
            + "  false as relhasrules,\n"
            + "  false as relhastriggers,\n"
            + "  false as relhassubclass,\n"
            + "  false as relrowsecurity,\n"
            + "  false as relforcerowsecurity,\n"
            + "  true as relispopulated,\n"
            + "  'n' as relreplident,\n"
            + "  false as relispartition,\n"
            + "  0 as relrewrite,\n"
            + "  0 as relfrozenxid,\n"
            + "  0 as relminmxid,\n"
            + "  '{}'::bigint[] as relacl,\n"
            + "  '{}'::text[] as reloptions,\n"
            + "  0 as relpartbound\n"
            + "from information_schema.tables t\n"
            + "inner join information_schema.columns using (table_catalog, table_schema, table_name)\n"
            + "group by t.table_name, t.table_schema\n"
            + "union all\n"
            + "select\n"
            + "    -1 as oid,\n"
            + "    i.index_name as relname,\n"
            + "    case table_schema when 'pg_catalog' then 11 when 'public' then 2200 else 0 end as relnamespace,\n"
            + "    0 as reltype,\n"
            + "    0 as reloftype,\n"
            + "    0 as relowner,\n"
            + "    1 as relam,\n"
            + "    0 as relfilenode,\n"
            + "    0 as reltablespace,\n"
            + "    0 as relpages,\n"
            + "    0.0::float8 as reltuples,\n"
            + "    0 as relallvisible,\n"
            + "    0 as reltoastrelid,\n"
            + "    false as relhasindex,\n"
            + "    false as relisshared,\n"
            + "    'p' as relpersistence,\n"
            + "    'r' as relkind,\n"
            + "    count(*) as relnatts,\n"
            + "    0 as relchecks,\n"
            + "    false as relhasrules,\n"
            + "    false as relhastriggers,\n"
            + "    false as relhassubclass,\n"
            + "    false as relrowsecurity,\n"
            + "    false as relforcerowsecurity,\n"
            + "    true as relispopulated,\n"
            + "    'n' as relreplident,\n"
            + "    false as relispartition,\n"
            + "    0 as relrewrite,\n"
            + "    0 as relfrozenxid,\n"
            + "    0 as relminmxid,\n"
            + "    '{}'::bigint[] as relacl,\n"
            + "    '{}'::text[] as reloptions,\n"
            + "    0 as relpartbound\n"
            + "from information_schema.indexes i\n"
            + "inner join information_schema.index_columns using (table_catalog, table_schema, table_name)\n"
            + "group by i.index_name, i.table_schema\n"
            + ")";

    @Override
    public String getTableExpression() {
      return PG_CLASS_CTE;
    }
  }

  private static class PgProc implements PgCatalogTable {
    private static final String PG_PROC_CTE =
        "pg_proc as (\n"
            + "select * from ("
            + "select 0::bigint as oid, ''::varchar as proname, 0::bigint as pronamespace, 0::bigint as proowner, "
            + "0::bigint as prolang, 0.0::float8 as procost, 0.0::float8 as prorows, 0::bigint as provariadic, "
            + "''::varchar as prosupport, ''::varchar as prokind, false::bool as prosecdef, false::bool as proleakproof, "
            + "false::bool as proisstrict, false::bool as proretset, ''::varchar as provolatile, ''::varchar as proparallel, "
            + "0::bigint as pronargs, 0::bigint as pronargdefaults, 0::bigint as prorettype, 0::bigint as proargtypes, "
            + "'{}'::bigint[] as proallargtypes, '{}'::varchar[] as proargmodes, '{}'::text[] as proargnames, "
            + "''::varchar as proargdefaults, '{}'::bigint[] as protrftypes, ''::text as prosrc, ''::text as probin, "
            + "''::varchar as prosqlbody, '{}'::text[] as proconfig, '{}'::bigint[] as proacl\n"
            + ") proc where false)";

    @Override
    public String getTableExpression() {
      return PG_PROC_CTE;
    }
  }

  private static class PgRange implements PgCatalogTable {
    private static final String PG_RANGE_CTE =
        "pg_range as (\n"
            + "select * from ("
            + "select 0::bigint as rngtypid, 0::bigint as rngsubtype, 0::bigint as rngmultitypid, "
            + "0::bigint as rngcollation, 0::bigint as rngsubopc, ''::varchar as rngcanonical, ''::varchar as rngsubdiff\n"
            + ") range where false)";

    @Override
    public String getTableExpression() {
      return PG_RANGE_CTE;
    }
  }

  @InternalApi
  public static class EmptyPgAttribute implements PgCatalogTable {
    private static final String PG_ATTRIBUTE_CTE =
        "pg_attribute as (\n"
            + "select * from ("
            + "select 0::bigint as attrelid, '' as attname, 0::bigint as atttypid, 0::bigint as attstattarget, "
            + "0::bigint as attlen, 0::bigint as attnum, 0::bigint as attndims, -1::bigint as attcacheoff, "
            + "0::bigint as atttypmod, true as attbyval, '' as attalign, '' as attstorage, '' as attcompression, "
            + "false as attnotnull, true as atthasdef, false as atthasmissing, '' as attidentity, '' as attgenerated, "
            + "false as attisdropped, true as attislocal, 0 as attinhcount, 0 as attcollation, '{}'::bigint[] as attacl, "
            + "'{}'::text[] as attoptions, '{}'::text[] as attfdwoptions, null as attmissingval\n"
            + ") a where false)";

    @Override
    public String getTableExpression() {
      return PG_ATTRIBUTE_CTE;
    }
  }

  private static class EmptyPgAttrdef implements PgCatalogTable {
    private static final String PG_ATTRDEF_CTE =
        "pg_attrdef as (\n"
            + "select * from ("
            + "select 0::bigint as oid, 0 as adrelid, 0::bigint as adnum, '' as adbin\n"
            + ") a where false)";

    @Override
    public String getTableExpression() {
      return PG_ATTRDEF_CTE;
    }
  }

  private static class EmptyPgEnum implements PgCatalogTable {
    private static final String PG_ENUM_CTE =
        "pg_enum as (\n"
            + "select * from ("
            + "select 0::bigint as oid, 0::bigint as enumtypid, 0.0::float8 as enumsortorder, ''::varchar as enumlabel\n"
            + ") e where false)";

    @Override
    public String getTableExpression() {
      return PG_ENUM_CTE;
    }
  }

  private static class EmptyPgConstraint implements PgCatalogTable {
    private static final String PG_CONSTRAINT_CTE =
        "pg_constraint as (\n"
            + "select * from ("
            + "select 0::bigint as oid, '' as conname, 0::bigint as connamespace, '' as contype, "
            + "false as condeferrable, false as condeferred, true as convalidated, 0 as conrelid, "
            + "0 as contypid, 0 as conindid, 0 as conparentid, 0 as confrelid, '' as confupdtype, "
            + "'' as confdeltype, '' as confmatchtype, true as conislocal, 0 as coninhcount, "
            + "true as connoinherit, '{}'::bigint[] as conkey, '{}'::bigint[] as confkey, "
            + "'{}'::bigint[] as conpfeqop, '{}'::bigint[] as conppeqop, '{}'::bigint[] as conffeqop, "
            + "'{}'::bigint[] as confdelsetcols, '{}'::bigint[] as conexclop, null as conbin\n"
            + ") c where false)";

    @Override
    public String getTableExpression() {
      return PG_CONSTRAINT_CTE;
    }
  }
}
