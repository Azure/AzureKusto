c_character <- function(...)
{
    x <- c(...)
    if (is_empty(x))
        return(character())

    if (!is.character(x))
        stop("Character input expected", call. = FALSE)

    x
}

#' Flag a character string as a Kusto identifier
#' @param ... character strings to flag as Kusto identifiers
ident <- function(...)
{
    x <- c_character(...)
    structure(x, class = c("ident", "character"))
}

setOldClass(c("ident", "character"), ident())
