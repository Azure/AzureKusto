#' Driver for Azure Data Explorer
#'
#' @keywords internal
#' @export
#' @import methods DBI
#' @importClassesFrom DBI DBIDriver DBIConnection
setClass("AzureDataExplorerDriver", contains = "DBIDriver")

#' @export
#' @rdname AzureDataExplorer-class
setMethod("dbUnloadDriver", "AzureDataExplorerDriver", function(drv, ...)
{
    TRUE
})

setMethod("show", "AzureDataExplorerDriver", function(object){
    cat("<AzureDataExplorerDriver>\n")
})

AzureDataExplorer <- function()
{
    new("AzureDataExplorerDriver")
}

#' AzureDataExplorer connection class.
#'
#' @export
#' @keywords internal
setClass("AzureDataExplorerConnection",
         contains = "DBIConnection",
         slots = list(
             host = "character",
             ptr = "externalptr"
         ))

#' @param drv An object created by AzureDataExplorer
#' @rdname AzureDataExplorer
#' @export
#' @examples
#' \dontrun{
#' db <- dbConnect(AzureDataExplorer::AzureDataExplorer())
#' dbWriteTable(db, "mtcars", mtcars)
#' dbGetQuery(db, "mtcars | where cyl == 4")
#' }
setMethod("dbConnect", "AzureDataExplorerDriver", function(drv, ...)
{
    new("AzureDataExplorerConnection", host=host, ...)
})

#' AzureDataExplorer results class
#'
#' @keywords internal
#' @export
setClass("AzureDataExplorerResult",
         contains="DBIResult",
         slots = list(ptr = "externalptr")
         )

#' Send a query to AzureDataExplorer.
#'
#' @export
#' @examples
#'
setMethod("dbSendQuery", "AzureDataExplorerConnection", function(conn, statement, ...)
{
    # TODO
    new("AzureDataExplorerResult", ...)
})

#' @export
setMethod("dbClearResult", "AzureDataExplorerResult", function(res, ...)
{
    # TODO: free resources
    TRUE
})

##' Retrieve records from query
##' @export
#setMethod("dbFetch", "AzureDataExplorerResult", function(res, n = -1, ...)
#{
    ##TODO
#})

##' @export
#setMethod("dbHasCompleted", "AzureDataExplorerResult", function(res, ...)
#{
    ##TODO
#})

