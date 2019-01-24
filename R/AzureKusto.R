#' @importFrom utils head
#' @import rlang
#' @import dplyr
NULL

utils::globalVariables(c("self", "asc"))

.onLoad <- function(libname, pkgname)
{
    add_methods()
    invisible(NULL)
}

