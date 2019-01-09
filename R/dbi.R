#' Driver for Azure Kusto
#'
#' @keywords internal
#' @export
#' @import methods DBI
#' @importClassesFrom DBI DBIDriver DBIConnection
setClass("AzureKustoDriver", contains = "DBIDriver")

#' @export
#' @rdname AzureKusto-class
setMethod("dbUnloadDriver", "AzureKustoDriver", function(drv, ...)
{
    TRUE
})

setMethod("show", "AzureKustoDriver", function(object){
    cat("<AzureKustoDriver>\n")
})

AzureKusto <- function()
{
    new("AzureKustoDriver")
}

#' AzureKusto connection class.
#'
#' @export
#' @keywords internal
setClass("AzureKustoConnection",
         contains = "DBIConnection",
         slots = list(
             host = "character",
             ptr = "externalptr"
         ))

#' @param drv An object created by AzureKusto
#' @rdname AzureKusto
#' @export
#' @examples
#' \dontrun{
#' db <- dbConnect(AzureKusto::AzureKusto())
#' dbWriteTable(db, "mtcars", mtcars)
#' dbGetQuery(db, "mtcars | where cyl == 4")
#' }
setMethod("dbConnect", "AzureKustoDriver", function(drv, ...)
{
    new("AzureKustoConnection", host=host, ...)
})

#' AzureKusto results class
#'
#' @keywords internal
#' @export
setClass("AzureKustoResult",
         contains="DBIResult",
         slots = list(ptr = "externalptr")
         )

#' Send a query to AzureKusto.
#'
#' @export
#' @examples
#'
setMethod("dbSendQuery", "AzureKustoConnection", function(conn, statement, ...)
{
    # TODO
    new("AzureKustoResult", ...)
})

#' @export
setMethod("dbClearResult", "AzureKustoResult", function(res, ...)
{
    # TODO: free resources
    TRUE
})

##' Retrieve records from query
##' @export
#setMethod("dbFetch", "AzureKustoResult", function(res, n = -1, ...)
#{
    ##TODO
#})

##' @export
#setMethod("dbHasCompleted", "AzureKustoResult", function(res, ...)
#{
    ##TODO
#})

