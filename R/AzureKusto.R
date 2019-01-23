#' @import rlang
#' @import dplyr
#' @import DBI
NULL

utils::globalVariables("self")

.onLoad <- function(libname, pkgname)
{
    add_methods()
    invisible(NULL)
}

