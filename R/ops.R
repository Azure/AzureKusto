#' @export
select.tbl <- function(.data, ...)
{
    dots <- quos(...)
    add_op("select", .data, dots = dots)
}

distinct.tbl <- function(.data, ...)
{
    dots <- quos(...)
    add_op("distinct", .data, dots = dots)
}

#' @export
add_op <- function(name, .data, dots = list(), args = list()) {
    .data$ops <- append(.data$ops, list(op(name, dots = dots, args = args)))
    return(.data)
}

#' @export
op <- function(name, dots = list(), args = list())
{
  structure(
    list(
      name = name,
      dots = dots,
      args = args
    ),
    class = c(paste0("op_", name), "op")
  )
}

#' @export
show_query.tbl <- function(tbl, ...)
{
    ops <- lapply(tbl$ops, function(x) render(x, tbl$table))
    tblname <- sprintf("database(%s).%s", tbl$db$db, tbl$name)
    q_str <- paste(tblname, ops, sep = "\n| ")
    cat(q_str, "\n")
    invisible(q_str)
}

#' @export
render <- function(query, con = NULL, ...)
{
    UseMethod("render")
}

#' @export
render.op_select <- function(op, tbl)
{
    cols <- tidyselect::vars_select(names(tbl), !!! op$dots)
    cols <- paste(cols, collapse=", ")
    paste0("project ", cols)
}

#' @export
render.op_distinct <- function(op, tbl)
{
    cols <- tidyselect::vars_select(names(tbl), !!! op$dots)
    cols <- paste(cols, collapse=", ")
    paste0("distinct ", cols)
}
