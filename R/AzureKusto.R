#' @importFrom utils head
#' @import rlang
#' @import dplyr
#' @importFrom tidyr nest unnest
#' @import DBI
#' @import methods
NULL

utils::globalVariables(c("self", "asc", "con", "make_list"))

.onLoad <- function(libname, pkgname)
{
    add_methods()
    invisible(NULL)
}
