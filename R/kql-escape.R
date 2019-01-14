#' Escape/quote a string.
#'
#' @param x An object to escape. Existing kql vectors will be left as is,
#'   character vectors are escaped with single quotes, numeric vectors have
#'   trailing `.0` added if they're whole numbers, identifiers are
#'   escaped with double quotes.
#' @param parens,collapse Controls behaviour when multiple values are supplied.
#'   `parens` should be a logical flag, or if `NA`, will wrap in
#'   parens if length > 1.
#'
#'   Default behaviour: lists are always wrapped in parens and separated by
#'   commas, identifiers are separated by commas and never wrapped,
#'   atomic vectors are separated by spaces and wrapped in parens if needed.
#' @export
#' @examples
#' # Doubles vs. integers
#' escape(1:5)
#' escape(c(1, 5.4))
#'
#' # String vs known kql vs. kql identifier
#' escape("X")
#' escape(kql("X"))
#' escape(ident("X"))
#'
#' # Escaping is idempotent
#' escape("X")
#' escape(escape("X"))
#' escape(escape(escape("X")))
escape <- function(x, parens = NA, collapse = " ")
{
    UseMethod("escape")
}

#' @export
escape.ident <- function(x, parens = FALSE, collapse = ", ")
{
    y <- kql_escape_ident(x)
    kql_vector(y, parens, collapse)
}

#' @export
escape.logical <- function(x, parens = NA, collapse = ", ")
{
    kql_vector(kql_escape_logical(x), parens, collapse)
}

#' @export
escape.factor <- function(x, parens = NA, collapse = ", ")
{
    escape(as.character(x), parens = parens, collapse = collapse)
}

#' @export
escape.Date <- function(x, parens = NA, collapse = ", ")
{
    escape(as.character(x), parens = parens, collapse = collapse)
}

#' @export
escape.POSIXt <- function(x, parens = NA, collapse = ", ")
{
    x <- strftime(x, "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")
    escape.character(x, parens = parens, collapse = collapse)
}

#' @export
escape.character <- function(x, parens = NA, collapse = ", ")
{
    # Kusto doesn't support null strings, instead use empty string
    out <- x
    out[is.na(x)] <- ""
    out[is.null(x)] <- ""
    kql_vector(kql_escape_string(out), parens, collapse)
}

#' @export
escape.double <- function(x, parens = NA, collapse = ", ")
{
    out <- as.character(x)
    out[is.na(x)] <- "real(null)"
    inf <- is.infinite(x)
    out[inf & x > 0] <- "'real(+inf)'"
    out[inf & x < 0] <- "'real(-inf)'"

    kql_vector(out, parens, collapse)
}

#' @export
escape.integer <- function(x, parens = NA, collapse = ", ")
{
    x[is.na(x)] <- "int(null)"
    kql_vector(x, parens, collapse)
}

#' @export
escape.integer64 <- function(x, parens = NA, collapse = ", ")
{
    x <- as.character(x)
    x[is.na(x)] <- "long(null)"
    kql_vector(x, parens, collapse)
}

#' @export
escape.NULL <- function(x, parens = NA, collapse = " ")
{
    kql("null")
}

#' @export
escape.kql <- function(x, parens = NULL, collapse = NULL)
{
    kql_vector(x, isTRUE(parens), collapse)
}

#' @export
escape.list <- function(x, parens = TRUE, collapse = ", ")
{
    pieces <- vapply(x, escape, character(1))
    kql_vector(pieces, parens, collapse)
}

#' @export
#' @rdname escape
kql_vector <- function(x, parens = NA, collapse = " ")
{
    if (is_empty(x))
    {
        if (!is.null(collapse))
            return(if (isTRUE(parens)) kql("()") else kql(""))
        else
            return(kql())
    }

    if (is.na(parens))
        parens <- length(x) > 1L

    x <- paste(x, collapse = collapse)

    if (parens)
        x <- paste0("(", x, ")")

    kql(x)
}

#' Build a KQL string.
#'
#' @param ... input to convert to KQL. Use [kql()] to preserve
#'   user input as is (dangerous), and [ident()] to label user
#'   input as kql identifiers (safe)
#' @param .env the environment in which to evalute the arguments. Should not
#'   be needed in typical use.
#' @export
build_kql <- function(..., .env = parent.frame())
{
    escape_expr <- function(x) {
        # If it's a string, leave it as is
        if (is.character(x)) return(x)

        val <- eval_bare(x, .env)
        # Skip nulls, so you can use if statements like in paste
        if (is.null(val)) return("")

        escape(val)
    }

    pieces <- vapply(dots(...), escape_expr, character(1))
    kql(paste0(pieces, collapse = ""))
}

#' Helper function for quoting kql elements.
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
    if (is_empty(x)) return(x)

    y <- gsub(quote, paste0(quote, quote), x, fixed = TRUE)
    y <- paste0(quote, y, quote)
    y[is.na(x)] <- "NULL"
    names(y) <- names(x)
    y
}

#' @export
kql_escape_string <- function(x)
{
    kql_quote(x, "'")
}

#' @export
kql_escape_ident <- function(x)
{
    x
}

#' @export
kql_escape_logical <- function(x)
{
    y <- tolower(as.character(x))
    y[is.na(x)] <- "null"
    y
}
