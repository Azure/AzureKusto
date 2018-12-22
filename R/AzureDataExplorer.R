#' @import AzureRMR
NULL


.onLoad <- function(libname, pkgname)
{
    config_dir <- config_dir()
    if(!dir.exists(config_dir))
        dir.create(config_dir, recursive=TRUE)

    add_methods()
    invisible(NULL)
}


config_dir <- function()
{
    rappdirs::user_config_dir(appname="AzureDataExplorer", appauthor="AzureR", roaming=FALSE)
}
