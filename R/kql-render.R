#' @export
kql_render <- function(query, ...)
{
    UseMethod("kql_render")
}

#' @export
kql_render.kql_query <- function(q)
{
    tblname <- sprintf("database('%s').%s", q$src$database, escape(ident(q$src$table)))
    q_str <- paste(unlist(q$ops[-1]), collapse = "\n| ")

    if (nchar(q_str) > 0)
        q_str <- kql(paste(tblname, q_str, sep="\n| "))
    else
        q_str <- tblname

    q_str
}

#' @export
kql_render.tbl_kusto_abstract <- function(query, ...)
{
    # only used for testing
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
