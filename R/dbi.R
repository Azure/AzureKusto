#' DBI interface to Kusto
#'
#' AzureKusto implements a subset of the DBI specification for interfacing with databases in R. The following methods are supported:
#' - Connections: [dbConnect], [dbDisconnect], [dbCanConnect]
#' - Table management: [dbExistsTable], [dbCreateTable], [dbRemoveTable], [dbReadTable], [dbWriteTable]
#' - Querying: [dbGetQuery], [dbSendQuery], [dbFetch], [dbSendStatement], [dbExecute], [dbListFields], [dbColumnInfo]
#'
#' Kusto is quite different to the SQL databases that DBI targets, which affects the behaviour of certain DBI methods and renders other moot.
#'
#' - Kusto is connectionless. `dbConnect` simply wraps a database endpoint object, created with [kusto_database_endpoint]. Similarly, `dbDisconnect` always returns TRUE. `dbCanConnect` attempts to check if querying the database will succeed, but this may not be accurate.
#'
#' - Temporary tables are not a Kusto concept, so `dbCreateTable(*, temporary=TRUE)` will throw an error.
#'
#' - It only supports synchronous queries, with a default timeout of 4 minutes. `dbSendQuery` and `dbSendStatement` will wait for the query to execute, rather than returning immediately. The object returned contains wraps the full result of the query, which `dbFetch` extracts.
#'
#' - The Kusto Query Language (KQL) is not SQL, and so higher-level SQL methods are not implemented.
#'
#' @name kusto-DBI
#' @aliases kusto-DBI kusto_dbi AzureKusto_dbi 
#' @rdname kusto-DBI
NULL

## subclasses

setOldClass("kusto_database_endpoint")


#' Kusto DBI driver class
#'
#' @keywords internal
#' @export
setClass("AzureKustoDriver", contains="DBIDriver")


#' Kusto DBI connection class
#'
#' @keywords internal
#' @export
setClass("AzureKustoConnection", contains="DBIConnection", slots=list(
    endpoint="kusto_database_endpoint"
))


#' Kusto DBI result class
#'
#' @keywords internal
#' @export
setClass("AzureKustoResult", contains="DBIResult", slots=list(
    data="data.frame"
))


## connection/server methods

setMethod("show", "AzureKustoConnection", function(object){
    cat("<AzureKustoConnection>\n")
    cat("Endpoint:\n")
    print(object@endpoint)
    invisible(object)
})


#' @rdname AzureKusto
#' @export
AzureKusto <- function()
{
    new("AzureKustoDriver")
}


#' DBI interface: connect to a Kusto cluster
#'
#' Functions to connect to a Kusto cluster.
#'
#' @param drv An AzureKusto DBI driver object, instantiated with `AzureKusto()`.
#' @param ... Authentication arguments supplied to `kusto_database_endpoint`.
#' @param bigint How to treat Kusto long integer columns. By default, they will be converted to R numeric variables. If this is "integer64", they will be converted to `integer64` variables using the bit64 package.
#' @param conn For `dbDisconnect`, an AzureKustoConnection object obtained with `dbConnect`.
#'
#' @details
#' Kusto is connectionless, so `dbConnect` simply wraps a database endpoint object, generated with `kusto_database_endpoint(...)`. The endpoint itself can be accessed via the `@endpoint` slot. Similarly, `dbDisconnect` always returns TRUE.
#'
#' `dbCanConnect` attempts to detect whether querying the database with the given information and credentials will be successful. The result may not be accurate; essentially all it does is check that its arguments are valid Kusto properties. Ultimately the best way to tell if querying will work is to try it.
#'
#' @return
#' For `dbConnect`, an object of class AzureKustoConnection.
#'
#' For `dbCanConnect`, TRUE if authenticating with the Kusto server succeeded with the given arguments, and FALSE otherwise.
#'
#' For `dbDisconnect`, always TRUE, invisibly.
#'
#' @seealso
#' [kusto-DBI], [dbReadTable], [dbWriteTable], [dbGetQuery], [dbSendStatement], [kusto_database_endpoint]
#'
#' @examples
#' \dontrun{
#' db <- DBI::dbConnect(AzureKusto(),
#'     server="https://mycluster.westus.kusto.windows.net", database="database", tenantid="contoso")
#'
#' DBI::dbDisconnect(db)
#'
#' # no authentication credentials: returns FALSE
#' DBI::dbCanConnect(AzureKusto(),
#'     server="https://mycluster.westus.kusto.windows.net")
#'
#' }
#' @aliases AzureKusto-connection
#' @rdname AzureKusto
#' @export
setMethod("dbConnect", "AzureKustoDriver", function(drv, ..., bigint=c("numeric", "integer64"))
{
    bigint <- match.arg(bigint)
    endpoint <- kusto_database_endpoint(..., .use_integer64=(bigint == "integer64"))
    new("AzureKustoConnection", endpoint=endpoint)
})

#' @rdname AzureKusto
#' @export
setMethod("dbCanConnect", "AzureKustoDriver", function(drv, ...)
{
    res <- try(dbConnect(drv, ...), silent=TRUE)
    !inherits(res, "try-error")
})

#' @rdname AzureKusto
#' @export
setMethod("dbDisconnect", "AzureKustoDriver", function(conn, ...)
{
    invisible(TRUE)
})


## table methods

#' DBI methods for Kusto table management
#'
#' @param conn An AzureKustoConnection object.
#' @param name A string containing a table name.
#' @param value For `dbWriteTable`, a data frame to be written to a Kusto table.
#' @param fields For `dbCreateTable`, the table specification: either a named character vector, or a data frame of sample values.
#' @param method For `dbWriteTable`, the ingestion method to use to write the table. See [ingest_local].
#' @param row.names For `dbCreateTable`, the row names. Not used.
#' @param temporary For `dbCreateTable`, whether to create a temporary table. Must be `FALSE` for Kusto.
#' @param ... Further arguments passed to `run_query`.
#'
#' @details
#' These functions read, write, create and delete a table, list the tables in a Kusto database, and check for table existence. With the exception of `dbWriteTable`, they ultimately call `run_query` which does the actual work of communicating with the Kusto server. `dbWriteTable` calls `ingest_local` to write the data to the server; note that it only supports ingesting a local data frame, as per the DBI spec.
#'
#' Kusto does not have the concept of temporary tables, so calling `dbCreateTable` with `temporary` set to anything other than `FALSE` will generate an error.
#'
#' `dbReadTable` and `dbWriteTable` are likely to be of limited use in practical scenarios, since Kusto tables tend to be much larger than available memory.
#'
#' @return
#' For `dbReadTable`, an in-memory data frame containing the table.
#'
#' @seealso
#' [AzureKusto-connection], [dbConnect], [run_query], [ingest_local]
#'
#' @examples
#' \dontrun{
#' db <- DBI::dbConnect(AzureKusto(),
#'     server="https://mycluster.location.kusto.windows.net", database="database"...)
#'
#' DBI::dbListTables(db)
#'
#' if(!DBI::dbExistsTable(db, "mtcars"))
#'     DBI::dbCreateTable(db, "mtcars")
#'
#' DBI::dbWriteTable(db, "mtcars", mtcars, method="inline")
#'
#' DBI::dbReadTable(db, "mtcars")
#'
#' DBI::dbRemoveTable(db, "mtcars")
#'
#' }
#' @rdname DBI_table
#' @export
setMethod("dbReadTable", c("AzureKustoConnection", "character"), function(conn, name, ...)
{
    if(!dbi_validate_table_name(name))
        stop("Must provide a table name, not an expression")
    run_query(conn@endpoint, escape(ident(name)))
})


#' @rdname DBI_table
#' @export
setMethod("dbWriteTable", "AzureKustoConnection", function(conn, name, value, method, ...)
{
    if(!dbi_validate_table_name(name))
        stop("Must provide a table name, not an expression")

    if(!dbExistsTable(conn, name))
        dbCreateTable(conn, name, value)

    ingest_local(conn@endpoint, value, name, method, ...)
    invisible(TRUE)
})


#' @rdname DBI_table
#' @export
setMethod("dbCreateTable", "AzureKustoConnection", function(conn, name, fields, ..., row.names=NULL, temporary=FALSE)
{
    build_fields <- function()
    {
        if(is.character(fields) &&
           !is.null(names(fields)) &&
           !any(is.na(names(fields))))
        {
            # build a dummy list of values
            # converting string "x" -> object of class x effectively guards against injection
            fields <- sapply(fields, function(value)
            {
                switch(tolower(value),
                    bool=logical(0),
                    int=integer(0),
                    date=, datetime=structure(numeric(0), class=c("POSIXct", "POSIXt")),
                    real=numeric(0),
                    long=structure(numeric(0), class="integer64"),
                    character(0))
            })
        }
        else if(!is.list(fields))
            stop("Bad fields specification", call.=FALSE)

        build_param_list(fields)
    }

    stopifnot(is.null(row.names))
    if(temporary)
        stop("Kusto does not have temporary tables")

    if(!dbi_validate_table_name(name))
        stop("Must provide a table name, not an expression")

    cmd <- paste(".create table",
        escape(ident(name)),
        build_fields())
    run_query(conn@endpoint, cmd)
})


#' @rdname DBI_table
#' @export
setMethod("dbRemoveTable", "AzureKustoConnection", function(conn, name, ...)
{
    if(!dbi_validate_table_name(name))
        stop("Must provide a table name, not an expression")

    cmd <- paste(".drop table", escape(ident(name)))
    run_query(conn@endpoint, cmd)
})


#' @rdname DBI_table
#' @export
setMethod("dbListTables", "AzureKustoConnection", function(conn, ...)
{
    res <- run_query(conn@endpoint, ".show tables")
    res$TableName
})


#' @rdname DBI_table
#' @export
setMethod("dbExistsTable", "AzureKustoConnection", function(conn, name, ...)
{
    if(!dbi_validate_table_name(name))
        stop("Must provide a table name, not an expression")

    name %in% dbListTables(conn)
})


## query methods


#' DBI methods for Kusto queries and commands
#'
#' @param conn An AzureKustoConnection object.
#' @param statement A string containing a Kusto query or control command.
#' @param res An AzureKustoResult resultset object
#' @param name For `dbListFields`, a table name.
#' @param n The number of rows to return. Not used.
#' @param ... Further arguments passed to `run_query`.
#'
#' @details
#' These are the basic DBI functions to query the database. Note that Kusto only supports synchronous queries and commands; in particular, `dbSendQuery` and `dbSendStatement` will wait for the query or statement to complete, rather than returning immediately.
#'
#' `dbSendStatement` and `dbExecute` are meant for running Kusto control commands, and will throw an error if passed a regular query. `dbExecute` also returns the entire result of running the command, rather than simply a row count.
#'
#' @seealso
#' [dbConnect], [dbReadTable], [dbWriteTable], [run_query]
#' @examples
#' \dontrun{
#'
#' db <- DBI::dbConnect(AzureKusto(),
#'     server="https://mycluster.location.kusto.windows.net", database="database"...)
#'
#' DBI::dbGetQuery(db, "iris | count")
#' DBI::dbListFields(db, "iris")
#'
#' # does the same thing as dbGetQuery, but returns an AzureKustoResult object
#' res <- DBI::dbSendQuery(db, "iris | count")
#' DBI::dbFetch(res)
#' DBI::dbColumnInfo(res)
#'
#' DBI::dbExecute(db, ".show tables")
#'
#' # does the same thing as dbExecute, but returns an AzureKustoResult object
#' res <- DBI::dbSendStatement(db, ".show tables")
#' DBI::dbFetch(res)
#'
#' }
#' @rdname DBI_query
#' @export
setMethod("dbGetQuery", c("AzureKustoConnection", "character"), function(conn, statement, ...)
{
    run_query(conn@endpoint, statement, ...)
})


#' @rdname DBI_query
#' @export
setMethod("dbSendQuery", "AzureKustoConnection", function(conn, statement, ...)
{
    res <- run_query(conn@endpoint, statement, ...)
    new("AzureKustoResult", data=res)
})


#' @rdname DBI_query
#' @export
setMethod("dbFetch", "AzureKustoResult", function(res, ...)
{
    res@data
})


#' @rdname DBI_query
#' @export
setMethod("dbSendStatement", c("AzureKustoConnection", "character"), function(conn, statement, ...)
{
    if(substr(statement, 1, 1) != ".")
        stop("dbSendStatement is for control commands only", call.=FALSE)
    res <- run_query(conn@endpoint, statement, ...)
    new("AzureKustoResult", data=res)
})


#' @rdname DBI_query
#' @export
setMethod("dbExecute", c("AzureKustoConnection", "character"), function(conn, statement, ...)
{
    if(substr(statement, 1, 1) != ".")
        stop("dbExecute is for control commands only", call.=FALSE)
    run_query(conn@endpoint, statement, ...)
})


#' @rdname DBI_query
#' @export
setMethod("dbListFields", c("AzureKustoConnection", "character"), function(conn, name, ...)
{
    if(!dbi_validate_table_name(name))
        stop("Must provide a table name, not an expression")

    cmd <- paste(".show table", escape(ident(name)))
    res <- run_query(conn@endpoint, cmd)
    res[[1]]
})


#' @rdname DBI_query
#' @export
setMethod("dbColumnInfo", "AzureKustoResult", function(res, ...)
{
    types <- sapply(res@data, function(x) class(x)[1])
    data.frame(names=names(types), types=types, stringsAsFactors=FALSE, row.names=NULL)
})


# helper function for dbXxxTable functions: check that argument is a name, not an expr
dbi_validate_table_name <- function(string)
{
    grepl("^[_a-zA-Z][_a-zA-Z0-9]*$", string)
}
