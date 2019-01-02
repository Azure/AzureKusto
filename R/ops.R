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

filter.tbl <- function(.data, ...)
{
    dots <- quos(...)
    add_op("filter", .data, dots = dots)
}

#' @export
add_op <- function(name, .data, dots = list(), args = list())
{
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
infix <- function(f)
{
    assertthat::assert_that(rlang::is_string(f))
    function(x, y)
    {
        paste(x, f, y, collapse=" ")
    }
}

#' @export
prefix <- function(f)
{
    assertthat::assert_that(rlang::is_string(f))
    function(...)
    {
        arglist <- paste(..., sep = ", ")
        paste0(f, "(", arglist ,")")
    }
}

#' @export
operator_env <- rlang::child_env(
  .parent = rlang::empty_env(),
  `!=`    = infix("!="),
  `==`    = infix("=="),
  `<`     = infix("<"),
  `<=`    = infix("<="),
  `>`     = infix(">"),
  `>=`    = infix(">="),
  sum     = prefix("sum")
)

kql_env <- function(expr, data)
{
    data_names = names(data)
    data_env <- rlang::as_environment(rlang::set_names(data_names))

    calls <- all_calls(expr)
    call_list <- purrr::map(rlang::set_names(calls), unknown_op)
    call_env <- rlang::as_environment(call_list, parent = data_env)

    op_env <- rlang::env_clone(operator_env, call_env)
    op_env

}

to_kql <- function(x, data)
{
    expr <- rlang::enexpr(x)
    out <- rlang::eval_bare(quote_strings(expr), kql_env(expr, data))
    kql(out)
}

kql_quote <- function(x, quote="'") {
  if (length(x) == 0) {
    return(x)
  }

  y <- gsub(quote, paste0(quote, quote), x, fixed = TRUE)
  y <- paste0(quote, y, quote)
  y[is.na(x)] <- "NULL"
  names(y) <- names(x)

  y
}

kql <- function(x) structure(x, class = "kql")

print.kql <- function(x)
{
    cat("<KQL> ", x, "\n", sep = "")
}

expr_type <- function(x)
{
    if (rlang::is_syntactic_literal(x)) {
        if (is.character(x))
        {
            "string"
        } else
        {
            "literal"
        }
    } else if (is.symbol(x))
    {
        "symbol"
    } else if (is.call(x))
    {
        "call"
    } else if (is.pairlist(x))
    {
        "pairlist"
    } else
    {
        typeof(x)
    }
}

switch_expr <- function(x, ...)
{
    switch(expr_type(x),
           ...,
           stop("Don't know how to handle type ", typeof(x), call. = FALSE)
           )
}

all_names_rec <- function(x)
{
    switch_expr(x,
                constant = character(),
                literal = character(),
                string = character(),
                symbol = as.character(x),
                call = purrr::flatten_chr(purrr::map(as.list(x[-1]), all_names))
                )
}

all_names <- function(x)
{
    unique(all_names_rec(x))
}

all_calls_rec <- function(x)
{
    switch_expr(x,
                constant = ,
                literal = ,
                string = ,
                symbol = character(),
                call = {
                    fname <- as.character(x[[1]])
                    children <- purrr::flatten_chr(purrr::map(as.list(x[-1]), all_calls))
                    c(fname, children)
                }
                )
}

all_calls <- function(x)
{
    unique(all_calls_rec(x))
}

quote_strings_rec <- function(x)
{
    if (expr_type(x) == "string")
    {
        kql_quote(x, "'")
    } else if (expr_type(x) == "call")
    {
        as.call(lapply(x, quote_strings))
    }
    else
    {
        x
    }
}

quote_strings <- function(x)
{
    quote_strings_rec(x)
}

unknown_op <- function(op) {
  rlang::new_function(
    rlang::exprs(... = ),
    rlang::expr({
      prefix(op)(...)
    })
  )
}

#' @export
show_query.tbl <- function(tbl, ...)
{
    ops <- unlist(lapply(tbl$ops, function(x) render(x, tbl$table)))
    tblname <- sprintf("database(%s).%s", tbl$db$db, tbl$name)
    q_str <- paste(ops, collapse = "\n| ")
    q_str <- paste(tblname, q_str, sep="\n| ")
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

#' @export
render.op_filter <- function(op, tbl)
{
    dots <- purrr::map(op$dots, rlang::get_expr)
    translated_dots <- purrr::map(dots, to_kql, data=tbl)
    paste0("where ", translated_dots)
}
