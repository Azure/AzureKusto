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
op <- function(name, dots = list(), args = list()) {
  structure(
    list(
      name = name,
      dots = dots,
      args = args
    ),
    class = c(paste0("op_", name), "op")
  )
}

### Cribbed from dbplyr

#' @export
show_query.tbl_lazy <- function(x, ...){
    qry <- kql_build(x, con=x$src, ...)
    kql_render(qry, con = x$src, ...)
}

#' @export
kql_render <- function(query, con = NULL, ...)
{
    UseMethod("kql_render")
}

#' @export
kql_render.tbl <- function(query, con = query$con, ...)
{
    q <- kql_build(query$ops, con = con, ...)
}

#' @export
kql_build <- function(op, con = NULL, ...)
{
    UseMethod("kql_build")
}

#' @export
kql_build.tbl <- function(op, con = NULL, ...)
{
    q <- kql_build(op$ops, con = con, ...)
}

#' @export
kql_build.op_select <- function(op, con, ...)
{
  vars <- tidyselect::vars_select(op_vars(op$x), !!! op$dots, .include = op_grps(op$x))
  select_query(kql_build(op$x, con), ident(vars))
}

#' @export
#' @rdname lazy_ops
op_vars <- function(op) UseMethod("op_vars")

#' @export
op_vars.op_base <- function(op) {
  op$vars
}
#' @export
op_vars.op_select <- function(op) {
  names(tidyselect::vars_select(op_vars(op$x), !!! op$dots, .include = op_grps(op$x)))
}

#' @export
#' @rdname lazy_ops
op_grps <- function(op) UseMethod("op_grps")
#' @export
op_grps.op_base <- function(op) character()

#' @export
op_grps.op_single <- function(op) {
  op_grps(op$x)
}

#' @export
#' @rdname kql_build
select_query <- function(from, select) {
  stopifnot(is.character(select))
  structure(
    list(
        from = from,
        select = select
    ),
    class = c("select_query", "query")
  )
}

#' @export
kql_render.select_query <- function(query, con = NULL, ..., root = FALSE)
{
  kql_select(con, query$select, ...)
}

escape <- function(x, parens = NA, collapse = " ", con = NULL) {
  UseMethod("escape")
}

#' Helper function for quoting query elements.
#'
#' If the quote character is present in the string, it will be doubled.
#' `NA`s will be replaced with NULL.
#'
#' @export
#' @param x Character vector to escape.
#' @param quote Single quoting character.
#' @export
#' @keywords internal
#' @examples
#' kql_quote("abc", "'")
#' kql_quote("I've had a good day", "'")
#' kql_quote(c("abc", NA), "'")
kql_quote <- function(x, quote)
{
    if (length(x) == 0) {
        return(x)
    }

    y <- gsub(quote, paste0(quote, quote), x, fixed = TRUE)
    y <- paste0(quote, y, quote)
    y[is.na(x)] <- "NULL"
    names(y) <- names(x)

    y
}

#' @export
kql_escape_string <- function(con, x)
{
    UseMethod("kql_escape_string")
}

#' @export
kql_escape_string.DBIConnection <- function(con, x)
{
    #kql_quote(x, "'")
    return(x)
}

#' @export
kql_escape_string.NULL <- function(con, x) {
    #kql_quote(x, "'")
    return(x)
}

#' @export
#' @rdname escape
kql_vector <- function(x, parens = NA, collapse = " ", con = NULL) {
  if (length(x) == 0) {
    if (!is.null(collapse)) {
      return(if (isTRUE(parens)) kql("()") else kql(""))
    } else {
      return(kql())
    }
  }

  if (is.na(parens)) {
      #parens <- length(x) > 1L
      parens <- FALSE
  }

  x <- paste(x, collapse = collapse)
  if (parens) x <- paste0("(", x, ")")
  kql(x)
}

#' @export
escape.character <- function(x, parens = NA, collapse = ", ", con = NULL) {
  kql_vector(kql_escape_string(con, x), parens, collapse, con = con)
}

#' @export
escape.list <- function(x, parens = TRUE, collapse = ", ", con = NULL) {
  pieces <- vapply(x, escape, character(1), con = con)
  kql_vector(pieces, parens, collapse)
}

#' @param ... Character vectors that will be combined into a single query.
#' @export
kql <- function(...) {
  x <- c_character(...)
  structure(x, class = c("kql", "character"))
}

#' @export
c.kql <- function(..., drop_null = FALSE, con = NULL) {
  input <- list(...)
  if (drop_null) input <- compact(input)

  out <- unlist(lapply(input, escape, collapse = NULL, con = con))
  kql(out)
}

#' @export
print.kql <- function(x, ...) cat(format(x, ...), sep = "\n| ")

#' @export
format.sql <- function(x, ...) {
  if (length(x) == 0) {
    paste0("<KQL> [empty]")
  } else {
    paste0("<KQL> ", x)
  }
}

dots <- function(...) {
  rlang::eval_bare(substitute(alist(...)))
}

c_character <- function(...) {
  x <- c(...)
  if (length(x) == 0) {
    return(character())
  }

  if (!is.character(x)) {
    stop("Character input expected", call. = FALSE)
  }

  x
}

build_kql <- function(..., .env = parent.frame(), con = NULL) {
  escape_expr <- function(x) {
    # If it's a string, leave it as is
    if (is.character(x)) return(x)

    val <- rlang::eval_bare(x, .env)
    # Skip nulls, so you can use if statements like in paste
    if (is.null(val)) return("")

    escape(val, con = con)
  }

  pieces <- vapply(dots(...), escape_expr, character(1))
  kql(paste0(pieces, collapse = ""))
}

kql_clause_select <- function(select, con){
  assertthat::assert_that(is.character(select))
  if (purrr::is_empty(select)) {
    abort("Query contains no columns")
  }

  build_kql(
    "| project ",
    escape(select, collapse = ", ", con = con)
  )
}

kql_clause_distinct <- function(select, con){
  assertthat::assert_that(is.character(select))
  if (purrr::is_empty(select)) {
    abort("Query contains no columns")
  }

  build_kql(
    "| distinct ",
    escape(select, collapse = ", ", con = con)
  )
}

#' @export
kql_select <- function(con, select, ...)
{
    UseMethod("kql_select")
}

#' @export
kql_select.DBIConnection <- function(con, select, ...)
{
  out <- list()
  out$select <- kql_clause_select(select, con)
  escape(unname(purrr::compact(out)), collapse = "\n| ", parens = FALSE, con = con)
}
