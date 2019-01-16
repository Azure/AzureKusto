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


#config_dir <- function()
#{
    #rappdirs::user_config_dir(appname="AzureKusto", appauthor="AzureR", roaming=FALSE)
#}
