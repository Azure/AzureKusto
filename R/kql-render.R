#' Render a set of operations on a tbl_kusto_abstract to a Kusto query
#' @param query The tbl_kusto instance with a sequence of operations in $ops
#' @param ... needed for agreement with generic. Not otherwise used.
#' @export
kql_render <- function(query, ...)
{
    UseMethod("kql_render")
}

#' @export
kql_render.kql_query <- function(query, ...)
{
    tblname <- sprintf("database('%s').%s", query$src$database, kql(query$src$table))
    q_str <- paste(unlist(query$ops[-1]), collapse = "\n| ")

    if (nchar(q_str) > 0)
        q_str <- kql(paste(tblname, q_str, sep="\n| "))
    else
        q_str <- tblname

    q_str
}

#' @export
kql_render.tbl_kusto_abstract <- function(query, ...)
{
    qry <- kql_build(query$ops, ...)
    kql_render(qry, ...)
}

#' @export
kql_render.op <- function(query, ...)
{
    kql_render(kql_build(query, ...), ...)
}

#' @export
kql_render.ident <- function(query, ..., root = TRUE)
{
    query
}

#' @export
kql_render.kql <- function(query, ...)
{
    query
}
