#' @importFrom utils head
#' @import rlang
#' @import dplyr
NULL

utils::globalVariables("self")

.onLoad <- function(libname, pkgname)
{
    add_methods()
    invisible(NULL)
}

