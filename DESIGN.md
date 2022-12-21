# Design for new dbplyr backend

Alex Kyllo

2022-12-21

The dplyr and dbplyr interfaces have evolved and dbplyr introduced significant
changes in v2, some of which are to keep up with upstream deprecations in rlang
and other tidyverse packages it depends on. Rather than maintaining an internal
"fork" of much of dbplyr's functionality, it makes more sense to take dbplyr as
a direct dependency and reuse as much of its implementation as possible, at
least the parts that aren't specific to the SQL language.

Kusto already has a DBI implementation, which should make this easier.

translate-sql.R contains the implementation of expression translation, which is
`translate_sql()`. This calls `translate_sql_()` (also exported), which calls
the `sql_translation` generic and some non-exported helpers like
`sql_data_mask`. If we can implement `sql_translation.AzureKustoConnection` and
call `dbplyr::translate_sql` or `translate_sql_` directly, then we can remove
much of the internals of the `AzureKusto` package.

`sql_translation` constructs and returns a `sql_variant` (which we imitated with
`kql_variant`), which takes three evaluation environments (built by calling
`sql_translator` with symbol mapping arguments) for scalar, aggregate, and
window contexts. Kusto does not have a window context concept, so we will need
to work around that.

escape.R contains generic S3 functions for escaping/quoting each data type.
`escape()` is called by `build_sql()`. It takes a `con` parameter for a
`DBIConnection`. The `escape()` generics call `sql_escape_*.DBIConnection`
generics that allow the `DBIConnection` instance to define its own
backend-specific escaping rules. The Kusto backend will probably need to
implement these, as well as the `DBI::dbQuoteIdentifier()` and
`DBI::dbQuoteString` generics.

`build_sql()` and `sql_expr()` both construct and return `sql` objects, which
have methods for printing.

`sql-build.R` contains functions for actually constructing and "rendering" the
SQL statement from the expression R data structure. `sql_render` is generic to
`tbl_lazy` and `lazy_query` and calls the `sql_build` generic, which takes
the `lazy_query` and constructs a `select_query` (`query-select.R`) which in turn
implements the `sql_render` generic.

The MSSQL backend `backend-mssql.R` for example implements the generic
`sql_query_select` by calling `sql_select_clauses`, which is non-exported.

The methods dbplyr adds for `DBIConnection` are:

- `db_analyze`
- `db_collect`
- `db_compute`
- `db_connection_describe`
- `db_copy_to`
- `db_create_index`
- `db_desc`
- `db_explain`
- `db_query_fields`
- `db_save_query`
- `db_sql_render`
- `db_table_temporary`
- `db_write_table`
- `dbplyr_fill`
- `get_col_types`
- `last_value_sql`
- `sql_escape_date`
- `sql_escape_datetime`
- `sql_escape_ident`
- `sql_escape_logical`
- `sql_escape_raw`
- `sql_escape_string`
- `sql_expr_matches`
- `sql_join_suffix`
- `sql_join`
- `sql_query_append`
- `sql_query_delete`
- `sql_query_explain`
- `sql_query_fields`
- `sql_query_insert`
- `sql_query_join`
- `sql_query_multi_join`
- `sql_query_rows`
- `sql_query_save`
- `sql_query_select`
- `sql_query_semi_join`
- `sql_query_set_op`
- `sql_query_update_from`
- `sql_query_upsert`
- `sql_query_wrap`
- `sql_random`
- `sql_returning_cols`
- `sql_select`
- `sql_semi_join`
- `sql_set_op`
- `sql_subquery`
- `sql_table_analyze`
- `sql_table_index`
- `sql_translate_env`
- `sql_translation`
- `sql_values_subquery`
- `supports_star_without_alias`
- `supports_window_clause`
- `values_prepare`