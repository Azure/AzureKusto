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
#' df <- tbl_kusto_abstract(df, "table1", src = simulate_kusto())
#' df %>% summarise(x = sd(x)) %>% show_query()
tbl_kusto_abstract <- function(df, table_name, src = NULL) {
  src$table <- table_name
  make_tbl("kusto_abstract", ops = op_base_local(df), src = src)
}

setOldClass(c("tbl_kusto_abstract", "tbl"))

#' @export
select.tbl_kusto_abstract <- function(.data, ...)
{
    dots <- quos(...)
    add_op_single("select", .data, dots = dots)
}

#' @export
distinct.tbl_kusto_abstract <- function(.data, ...)
{
    dots <- quos(...)
    add_op_single("distinct", .data, dots = dots)
}

#' @export
rename.tbl_kusto_abstract <- function(.data, ...)
{
    dots <- quos(...)
    add_op_single("rename", .data, dots = dots)
}

#' @export
filter.tbl_kusto_abstract <- function(.data, ...)
{
    dots <- quos(...)
    add_op_single("filter", .data, dots = dots)
}

#' @export
mutate.tbl_kusto_abstract <- function(.data, ...)
{
    dots <- quos(..., .named=TRUE)
    add_op_single("mutate", .data, dots = dots)
}

#' @export
arrange.tbl_kusto_abstract <- function(.data, ...)
{
    dots <- quos(...)
    names(dots) <- NULL
    add_op_single("arrange", .data, dots = dots)
}

#' @export
group_by.tbl_kusto_abstract <- function(.data, ..., add = FALSE)
{
    dots <- quos(...)

    if (is_empty(dots))
    {
        return(.data)
    }

    groups <- group_by_prepare(.data, .dots = dots, add = add)
    names <- vapply(groups$groups, as_string, character(1))
    add_op_single("group_by",
                  groups$data,
                  dots = set_names(groups$groups, names),
                  args = list(add = FALSE))
}

#' @export
ungroup.tbl_kusto_abstract <- function(.data, ...)
{
    add_op_single("ungroup", .data)
}

#' @export
summarise.tbl_kusto_abstract <- function(.data, ...)
{
    dots <- quos(..., .named = TRUE)
    add_op_single("summarise", .data, dots = dots)
}

#' @export
head.tbl_kusto_abstract <- function(x, n = 6L, ...)
{
    add_op_single("head", x, args = list(n = n))
}

#' @export
inner_join.tbl_kusto_abstract <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...)
{
    add_op_join("inner_join", x, y, by = by, suffix = suffix, ...)
}

#' @export
left_join.tbl_kusto_abstract <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...)
{
    add_op_join("left_join", x, y, by = by, suffix = suffix, ...)
}

#' @export
right_join.tbl_kusto_abstract <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...)
{
    add_op_join("right_join", x, y, by = by, suffix = suffix, ...)
}

#' @export
full_join.tbl_kusto_abstract <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...)
{
    add_op_join("full_join", x, y, by = by, suffix = suffix, ...)
}

#' @export
semi_join.tbl_kusto_abstract <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...)
{
    add_op_join("semi_join", x, y, by = by, suffix = suffix, ...)
}

#' @export
anti_join.tbl_kusto_abstract <- function(x, y, by = NULL, suffix = c(".x", ".y"), ...)
{
    add_op_join("anti_join", x, y, by = by, suffix = suffix, ...)
}

#' @export
union_all.tbl_kusto_abstract <- function(x, y, ...)
{
    add_op_set_op(x, y, "union_all")
}

#' @export
union.tbl_kusto_abstract <- function(x, y, ...)
{
    stop("Kusto does not support union(). Please use union_all() instead.")
}

#' @export
setdiff.tbl_kusto_abstract <- function(x, y, ...)
{
    stop("Kusto does not support setdiff() at this time.")
}

#' @export
setequal.tbl_kusto_abstract <- function(x, y, ...)
{
    stop("Kusto does not support setequal() at this time.")
}

#' @export
intersect.tbl_kusto_abstract <- function(x, y, ...)
{
    stop("Kusto does not support intersect() at this time.")
}

#' @export
tbl_vars.tbl_kusto_abstract <- function(x)
{
    op_vars(x$ops)
}

#' Simulate a kusto database endpoint as the source for a tbl_kusto_abstract.
#' @export
simulate_kusto <- function()
{
    structure(
        list(
            database = "local_df",
            cluster = "local_df"
        ),
        class = "kusto_database_endpoint"
    )
}

#' Translate a sequence of dplyr operations on a tbl into a Kusto query string.
#' @export
#' @param tbl A tbl_kusto or tbl_kusto_abstract instance
show_query.tbl_kusto_abstract <- function(tbl)
{
    qry <- kql_build(tbl)
    kql_render(qry)
}

#' A tbl object representing a table in a Kusto database.
#' @export
#' @param kusto_database An instance of kusto_database_endpoint that this table should be queried from
#' @param table_name The name of the table in the Kusto database
#' @param ... parameters to pass in case the Kusto source table is a parameterized function.
tbl_kusto <- function(kusto_database, table_name, ...)
{
    stopifnot(inherits(kusto_database, "kusto_database_endpoint"))
    params <- list(...)
    kusto_database$table <- table_name
    query_str <- sprintf("%s | take 1", escape(ident(table_name)))
    vars <- names(run_query(kusto_database, query_str, ...))
    ops <- op_base_remote(table_name, vars)
    make_tbl(c("kusto", "kusto_abstract"), src = kusto_database, ops = ops, params = params)
}

#' Compile the preceding dplyr oprations into a kusto query, execute it on the remote server,
#' and return the result as a tibble.
#' @export
#' @param tbl An instance of class tbl_kusto representing a Kusto table
#' @param ... needed for agreement with generic. Not otherwise used.
collect.tbl_kusto <- function(tbl, ...)
{
    q <- kql_build(tbl)
    q_str <- kql_render(q)
    params <- c(tbl$params, list(...))
    params$database <- tbl$src
    params$query <- q_str
    res <- do.call(run_query, params)
    as_tibble(res)
}
