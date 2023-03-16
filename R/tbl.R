#' Create a local lazy tbl
#'
#' Useful for testing KQL generation without a remote connection.
#'
#' @keywords internal
#' @export
#' @examples
#' library(dplyr)
#' df <- data.frame(x = 1, y = 2)
#'
#' df <- tbl_kusto_abstract(df, "table1")
#' df %>%
#'     summarise(x = sd(x)) %>%
#'     show_query()
tbl_kusto_abstract <- function(df, table_name, ...) {
    params <- list(...)
    src <- structure(
        list(
            database = "local_df",
            server = "local_df",
            table = escape(ident(table_name))
        ),
        class = "kusto_database_endpoint"
    )
    make_tbl("kusto_abstract", ops = op_base_local(df), src = src, params = params)
}

setOldClass(c("tbl_kusto_abstract", "tbl"))

#' @export
select.tbl_kusto_abstract <- function(.data, ...) {
    dots <- quos(...)
    add_op_single("select", .data, dots = dots)
}

#' @export
distinct.tbl_kusto_abstract <- function(.data, ...) {
    dots <- quos(...)
    dots <- partial_eval(dots, vars = op_vars(.data))
    add_op_single("distinct", .data, dots = dots)
}

#' @export
rename.tbl_kusto_abstract <- function(.data, ...) {
    dots <- quos(...)
    add_op_single("rename", .data, dots = dots)
}

#' @export
filter.tbl_kusto_abstract <- function(.data, ...) {
    dots <- quos(...)
    # add the tbl params into the environment of the expression's quosure
    dots <- lapply(dots, add_params_to_quosure, params = .data$params)
    dots <- partial_eval(dots, vars = op_vars(.data))
    add_op_single("filter", .data, dots = dots)
}

#' @export
mutate.tbl_kusto_abstract <- function(.data, ...) {
    dots <- quos(..., .named = TRUE)
    dots <- lapply(dots, add_params_to_quosure, params = .data$params)
    dots <- partial_eval(dots, vars = op_vars(.data))
    add_op_single("mutate", .data, dots = dots)
}

#' @export
arrange.tbl_kusto_abstract <- function(.data, ...) {
    dots <- quos(...)
    dots <- partial_eval(dots, vars = op_vars(.data))
    names(dots) <- NULL
    add_op_single("arrange", .data, dots = dots)
}

#' @export
group_by.tbl_kusto_abstract <- function(.data, ..., add = FALSE) {
    dots <- quos(...)
    dots <- partial_eval(dots, vars = op_vars(.data))
    if (is_empty(dots)) {
        return(.data)
    }
    # Updated for dplyr deprecation of .dots and add params
    groups <- group_by_prepare(.data, !!!dots, .add = add)
    names <- vapply(groups$groups, as_string, character(1))
    add_op_single("group_by",
        groups$data,
        dots = set_names(groups$groups, names),
        args = list(add = FALSE)
    )
}

#' @export
ungroup.tbl_kusto_abstract <- function(x, ...) {
    add_op_single("ungroup", x)
}

#' Summarise method for Kusto tables
#'
#' This method is the same as other summarise methods, with the exception of the `.strategy`, `.shufflekeys` and `.num_partitions` optional arguments. They provide hints to the Kusto engine on how to execute the summarisation, and can sometimes be useful to speed up a query. See the Kusto documentation for more details.
#'
#' @param .data A Kusto tbl.
#' @param ... Summarise expressions.
#' @param .strategy A summarise strategy to pass to Kusto. Currently the only value supported is "shuffle".
#' @param .shufflekeys A character vector of column names to use as shuffle keys.
#' @param .num_partitions The number of partitions for a shuffle query.
#' @seealso
#' [dplyr::summarise]
#'
#' @examples
#' \dontrun{
#'
#' tbl1 <- tbl_kusto(db, "table1")
#'
#' ## standard dplyr syntax:
#' summarise(tbl1, mx = mean(x))
#'
#' ## Kusto extensions:
#' summarise(tbl1, mx = mean(x), .strategy = "broadcast") # a broadcast summarise
#'
#' summarise(tbl1, mx = mean(x), .shufflekeys = c("var1", "var2")) # a shuffle summarise with keys
#'
#' summarise(tbl1, mx = mean(x), .num_partitions = 5) # no. of partitions for a shuffle summarise
#' }
#'
#' @rdname summarise
#' @export
summarise.tbl_kusto_abstract <- function(.data, ..., .strategy = NULL, .shufflekeys = NULL, .num_partitions = NULL) {
    dots <- quos(..., .named = TRUE)
    dots <- partial_eval(dots, vars = op_vars(.data))
    add_op_single("summarise", .data,
        dots = dots,
        args = list(.strategy = .strategy, .shufflekeys = .shufflekeys, .num_partitions = .num_partitions)
    )
}

#' Unnest method for Kusto tables
#'
#' This method takes a list column and expands it so that each element of the list gets its own row.
#' unnest() translates to Kusto's mv-expand operator.
#'
#' @param data A Kusto tbl.
#' @param cols Specification of columns to unnest.
#' @param ... `r lifecycle::badge("deprecated")`:
#'   previously you could write `df %>% unnest(x, y, z)`.
#'   Convert to `df %>% unnest(c(x, y, z))`. If you previously created a new
#'   variable in `unnest()` you'll now need to do it explicitly with `mutate()`.
#'   Convert `df %>% unnest(y = fun(x, y, z))`
#'   to `df %>% mutate(y = fun(x, y, z)) %>% unnest(y)`.
#' @param keep_empty Needed for agreement with generic. Not otherwise used. Kusto does not keep empty rows.
#' @param ptype Needed for agreement with generic. Not otherwise used.
#' @param names_sep Needed for agreement with generic. Not otherwise used.
#' @param names_repair Needed for agreement with generic. Not otherwise used.
#' @param .drop Needed for agreement with generic. Not otherwise used.
#' @param .id Data frame identifier - if supplied, will create a new column with name .id, giving a unique identifier. This is most useful if the list column is named.
#' @param .sep Needed for agreement with generic. Not otherwise used.
#' @param .preserve Needed for agreement with generic. Not otherwise used.
#' @export
unnest.tbl_kusto_abstract <- function(data, cols, ..., keep_empty = FALSE, ptype = NULL,
                                      names_sep = NULL, names_repair = NULL, .drop = NULL,
                                      .id = NULL, .sep = NULL, .preserve = NULL) {
    # dots <- quos(...)
    dots <- enquo(cols)
    add_op_single("unnest", data, dots = dots, args = list(.id = .id))
}

#' Nest method for Kusto tables
#'
#' This method collapses a column into a list
#'
#' @param .data A kusto tbl.
#' @param ... Specification of columns to nest. Translates to summarize make_list() in Kusto.
#' @export
nest.tbl_kusto_abstract <- function(.data, ...) {
    nest_vars <- unname(tidyselect::vars_select(op_vars(.data), ...))

    if (is_empty(nest_vars)) {
        nest_vars <- op_vars(.data)
    }

    group_vars <- union(op_grps(.data), setdiff(op_vars(.data), nest_vars))
    nest_vars <- setdiff(nest_vars, group_vars)
    dot_calls <- mapply(function(x) expr(make_list(!!as.name(x))), nest_vars)

    if (is_empty(group_vars)) {
        summarise(.data, !!!dot_calls)
    } else {
        summarise(group_by(.data, !!as.name(group_vars)), !!!dot_calls)
    }
}

#' @export
head.tbl_kusto_abstract <- function(x, n = 6L, ...) {
    add_op_single("head", x, args = list(n = n))
}

#' @export
slice_sample.tbl_kusto_abstract <- function(.data, ..., n = 6L, prop, by, weight_by, replace) {
    add_op_single("slice_sample", .data, args = list(n = n))
}

#' Join methods for Kusto tables
#'
#' These methods are the same as other joining methods, with the exception of the `.strategy`, `.shufflekeys` and `.num_partitions` optional arguments. They provide hints to the Kusto engine on how to execute the join, and can sometimes be useful to speed up a query. See the Kusto documentation for more details.
#'
#' @param x,y Kusto tbls.
#' @param by The columns to join on.
#' @param copy Needed for agreement with generic. Not otherwise used.
#' @param suffix The suffixes to use for deduplicating column names.
#' @param ... Other arguments passed to lower-level functions.
#' @param keep Needed for agreement with generic. Not otherwise used. Kusto retains keys from both sides of joins.
#' @param .strategy A join strategy hint to pass to Kusto. Currently the values supported are "shuffle" and "broadcast".
#' @param .shufflekeys A character vector of column names to use as shuffle keys.
#' @param .num_partitions The number of partitions for a shuffle query.
#' @param .remote A join strategy hint to use for cross-cluster joins. Can be "left", "right", "local" or "auto" (the default).
#' @seealso
#' [dplyr::join]
#'
#' @examples
#' \dontrun{
#'
#' tbl1 <- tbl_kusto(db, "table1")
#' tbl2 <- tbl_kusto(db, "table2")
#'
#' # standard dplyr syntax:
#' left_join(tbl1, tbl2)
#'
#' # Kusto extensions:
#' left_join(tbl1, tbl2, .strategy = "broadcast") # a broadcast join
#'
#' left_join(tbl1, tbl2, .shufflekeys = c("var1", "var2")) # shuffle join with shuffle keys
#'
#' left_join(tbl1, tbl2, .num_partitions = 5) # no. of partitions for a shuffle join
#' }
#'
#' @aliases inner_join left_join right_join full_join semi_join anti_join
#'
#' @rdname join
#' @export
inner_join.tbl_kusto_abstract <- function(x, y, by = NULL, copy = NULL, suffix = c(".x", ".y"), ...,
                                          keep = NULL, .strategy = NULL, .shufflekeys = NULL,
                                          .num_partitions = NULL, .remote = NULL) {
    add_op_join("inner_join", x, y,
        by = by, suffix = suffix,
        .strategy = .strategy, .shufflekeys = .shufflekeys, .num_partitions = .num_partitions,
        .remote = .remote, ...
    )
}

#' @rdname join
#' @export
left_join.tbl_kusto_abstract <- function(x, y, by = NULL, copy = NULL, suffix = c(".x", ".y"), ...,
                                         keep = NULL, .strategy = NULL, .shufflekeys = NULL,
                                         .num_partitions = NULL, .remote = NULL) {
    add_op_join("left_join", x, y,
        by = by, suffix = suffix,
        .strategy = .strategy, .shufflekeys = .shufflekeys, .num_partitions = .num_partitions,
        .remote = .remote, ...
    )
}

#' @rdname join
#' @export
right_join.tbl_kusto_abstract <- function(x, y, by = NULL, copy = NULL, suffix = c(".x", ".y"), ...,
                                          keep = NULL, .strategy = NULL, .shufflekeys = NULL,
                                          .num_partitions = NULL, .remote = NULL) {
    add_op_join("right_join", x, y,
        by = by, suffix = suffix,
        .strategy = .strategy, .shufflekeys = .shufflekeys, .num_partitions = .num_partitions,
        .remote = .remote, ...
    )
}

#' @rdname join
#' @export
full_join.tbl_kusto_abstract <- function(x, y, by = NULL, copy = NULL, suffix = c(".x", ".y"), ...,
                                         keep = NULL, .strategy = NULL, .shufflekeys = NULL,
                                         .num_partitions = NULL, .remote = NULL) {
    add_op_join("full_join", x, y,
        by = by, suffix = suffix,
        .strategy = .strategy, .shufflekeys = .shufflekeys, .num_partitions = .num_partitions,
        .remote = .remote, ...
    )
}

#' @rdname join
#' @export
semi_join.tbl_kusto_abstract <- function(x, y, by = NULL, copy = NULL, ..., suffix = c(".x", ".y"),
                                         .strategy = NULL, .shufflekeys = NULL, .num_partitions = NULL,
                                         .remote = NULL) {
    add_op_join("semi_join", x, y,
        by = by, suffix = suffix,
        .strategy = .strategy, .shufflekeys = .shufflekeys, .num_partitions = .num_partitions,
        .remote = .remote, ...
    )
}

#' @rdname join
#' @export
anti_join.tbl_kusto_abstract <- function(x, y, by = NULL, copy = NULL, suffix = c(".x", ".y"),
                                         .strategy = NULL, .shufflekeys = NULL, .num_partitions = NULL,
                                         .remote = NULL, ...) {
    add_op_join("anti_join", x, y,
        by = by, suffix = suffix,
        .strategy = .strategy, .shufflekeys = .shufflekeys, .num_partitions = .num_partitions,
        .remote = .remote, ...
    )
}

#' @export
union_all.tbl_kusto_abstract <- function(x, y, ...) {
    add_op_set_op(x, y, "union_all")
}

#' @export
union.tbl_kusto_abstract <- function(x, y, ...) {
    stop("Kusto does not support union(). Please use union_all() instead.")
}

#' @export
setdiff.tbl_kusto_abstract <- function(x, y, ...) {
    stop("Kusto does not support setdiff() at this time.")
}

#' @export
setequal.tbl_kusto_abstract <- function(x, y, ...) {
    stop("Kusto does not support setequal() at this time.")
}

#' @export
intersect.tbl_kusto_abstract <- function(x, y, ...) {
    stop("Kusto does not support intersect() at this time.")
}

#' @export
tbl_vars.tbl_kusto_abstract <- function(x) {
    op_vars(x$ops)
}

#' @export
group_vars.tbl_kusto_abstract <- function(x) {
    op_grps(x$ops)
}

#' Translate a sequence of dplyr operations on a tbl into a Kusto query string.
#' @export
#' @param x A tbl_kusto or tbl_kusto_abstract instance
#' @param ... needed for agreement with generic. Not otherwise used.
show_query.tbl_kusto_abstract <- function(x, ...) {
    qry <- kql_build(x)
    kql_render(qry)
}

#' A tbl object representing a table in a Kusto database.
#' @export
#' @param kusto_database An instance of kusto_database_endpoint that this table should be queried from
#' @param table_name The name of the table in the Kusto database
#' @param ... parameters to pass in case the Kusto source table is a parameterized function.
tbl_kusto <- function(kusto_database, table_name, ...) {
    stopifnot(inherits(kusto_database, "kusto_database_endpoint"))
    params <- list(...)
    # in case the table name is a function like MyFunction(arg1, arg2) we need to split it
    table_ident <- strsplit(table_name, split = "\\(")[[1]]
    table_ident[1] <- escape(ident(table_ident[1]))
    escaped_table_name <- paste(table_ident, collapse = "(")
    kusto_database$table <- escaped_table_name
    query_str <- sprintf("%s | take 1", escaped_table_name)
    vars <- names(run_query(kusto_database, query_str, ...))
    ops <- op_base_remote(table_name, vars)
    make_tbl(c("kusto", "kusto_abstract"), src = kusto_database, ops = ops, params = params)
}

#' Compile the preceding dplyr operations into a kusto query, execute it on the remote server,
#' and return the result as a tibble.
#' @export
#' @param x An instance of class tbl_kusto representing a Kusto table
#' @param ... needed for agreement with generic. Not otherwise used.
collect.tbl_kusto <- function(x, ...) {
    q <- kql_build(x)
    q_str <- kql_render(q)
    params <- c(x$params, list(...))
    params$database <- x$src
    params$qry_cmd <- q_str
    res <- do.call(run_query, params)
    tibble::as_tibble(res)
}

generate_table_name <- function() {
    paste0("Rtbl_", paste0(sample(letters, 8), collapse = ""))
}

#' Execute the query, store the results in a table, and return a reference to the new table
#' @export
#' @param x An instance of class tbl_kusto representing a Kusto table
#' @param ... other parameters passed to the query
#' @param name The name for the Kusto table to be created.
#' If name is omitted, the table will be named Rtbl_ + 8 random lowercase letters
compute.tbl_kusto <- function(x, ..., name = generate_table_name()) {
    q <- kql_build(x)
    q_str <- kql_render(q)
    new_tbl_name <- kql_escape_ident(name)
    set_cmd <- kql(paste0(".set ", new_tbl_name, " <|\n"))
    q_str <- kql(paste0(set_cmd, q_str))
    params <- c(x$params, list(...))
    params$database <- x$src
    params$qry_cmd <- q_str
    res <- do.call(run_query, params)
    invisible(tbl_kusto(x$src, name))
}

#' Execute the query, store the results in a table, and return a reference to the new table
#' Run a Kusto query and export results to Azure Storage in Parquet or CSV
#' format.
#'
#' @param query The text of the Kusto query to run
#' @param storage_uri The URI of the blob storage container to export to
#' @param name_prefix The filename prefix for each exported file
#' @param key The account key for the storage container.
#' uses the identity that is signed into Kusto to authenticate to Azure Storage.
#' @param format Options are "parquet", "csv", "tsv", "json"
#' @param distributed logical, indicates whether Kusto should distributed the
#' export job to multiple nodes, in which case multiple files will be written
#' to storage concurrently.
kusto_export_cmd <- function(
    query, storage_uri, name_prefix, key, format,
    distributed) {
    # Make sure the storage uri ends with a slash
    if (!(format %in% c("parquet", "csv", "tsv", "json"))) {
        stop("Format must be one of parquet, csv, tsv, or json.")
    }
    if (!endsWith(storage_uri, "/")) {
        storage_uri <- paste0(storage_uri, "/")
    }
    distr <- ifelse(distributed, "true", "false")
    compr <- ifelse(format == "parquet", "snappy", "gzip")
    sprintf(".export
compressed
to %s (h@'%s%s;%s')
with (
sizeLimit=1073741824,
namePrefix='%s',
fileExtension='%s',
compressionType='%s',
includeHeaders='firstFile',
encoding='UTF8NoBOM',
distributed=%s
)
<|
%s
", format, storage_uri, name_prefix, key, name_prefix, format, compr, distr, query)
}

#' Execute the Kusto query and export the result to Azure Storage.
#' @param tbl An object representing a table or database.
#' @param storage_uri The Azure Storage URI to export files to.
#' @param query A Kusto query string
#' @param name_prefix The filename prefix to use for exported files.
#' @param key default "impersonate" which uses the account signed into Kusto to
#' authenticate to Azure Storage. An Azure Storage account key.
#' @param format Options are "parquet", "csv", "tsv", "json"
#' @param distributed logical, indicates whether Kusto should distributed the
#' export job to multiple nodes, in which case multiple files will be written
#' to storage concurrently.
#' @param ... needed for agreement with generic. Not otherwise used.
#' @rdname export
#' @export
export <- function(
    tbl, storage_uri, query = NULL, name_prefix = "export",
    key = "impersonate", format = "parquet", distributed = FALSE, ...) {
    UseMethod("export")
}

#' Execute the Kusto query and export the result to Azure Storage.
#' @param tbl A Kusto database endpoint object, as returned by `kusto_database_endpoint`.
#' @param query A Kusto query string
#' @param storage_uri The Azure Storage URI to export files to.
#' @param name_prefix The filename prefix to use for exported files.
#' @param key default "impersonate" which uses the account signed into Kusto to
#' authenticate to Azure Storage. An Azure Storage account key.
#' @param format Options are "parquet", "csv", "tsv", "json"
#' @param distributed logical, indicates whether Kusto should distributed the
#' export job to multiple nodes, in which case multiple files will be written
#' to storage concurrently.
#' @param ... needed for agreement with generic. Not otherwise used.
#' @rdname export
#' @export
export.kusto_database_endpoint <- function(
    tbl, storage_uri, query = NULL, name_prefix = "export",
    key = "impersonate", format = "parquet", distributed = FALSE, ...) {
    if (missing(query)) stop("query parameter is required.")
    is_cmd <- substr(query, 1, 1) == "."
    if (is_cmd) stop("Management commands cannot be used with export()")
    q_str <- kusto_export_cmd(
        query = query, storage_uri = storage_uri,
        name_prefix = name_prefix, key = key, format = format,
        distributed = distributed
    )
    run_query(tbl, q_str, ...)
}

#' @rdname export
#' @export
export.tbl_kusto <- function(
    tbl, storage_uri, query = NULL, name_prefix = "export",
    key = "impersonate", format = "parquet", distributed = FALSE, ...) {
    database <- tbl$src
    q <- kql_render(kql_build(tbl))
    q_str <- kusto_export_cmd(
        query = q, storage_uri = storage_uri,
        name_prefix = name_prefix, key = key, format = format,
        distributed = distributed
    )
    res <- run_query(database, q_str, ...)
    tibble::as_tibble(res)
}

#' @keywords internal
#' @export
print.tbl_kusto_abstract <- function(x, ...) {
    # different paths if this is a query, simulated table, or real table
    if (!inherits(x$ops, "op_base")) {
        cat("<Kusto query>\n")
        print(show_query(x))
    } else if (!inherits(x, "tbl_kusto")) {
        cat("<Simulated Kusto table '")
        name <- paste0("local_df/", x$src$table)
        cat(name, "'>\n", sep = "")
    } else {
        cat("<Kusto table '")
        url <- httr::parse_url(x$src$server)
        url$path <- file.path(x$src$database, x$src$table)
        cat(httr::build_url(url), "'>\n", sep = "")
    }

    invisible(x)
}

add_params_to_quosure <- function(quosure, params) {
    new_env <- list2env(params, envir = get_env(quosure))
    quo_set_env(quosure, new_env)
}
