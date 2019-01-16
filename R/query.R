#' @export
run_query <- function(database, ...)
{
    UseMethod("run_query")
}


#' @export
run_query.kusto_database_endpoint <- function(database, query, ...)
{
    server <- database$server
    user <- database$user
    password <- database$pwd

    # token can be a string or an object of class AzureRMR::AzureToken
    token <- if(AzureRMR::is_azure_token(database$token))
        database$token$credentials$access_token
    else if(is.character(database$token))
        database$token
    else stop("Invalid authentication token in database endpoint", call.=FALSE)

    uri <- paste0(server, "/v1/rest/query")
    parse_query_result(call_kusto(token, user, password, uri, database$database, query, ...))
}


#' @export
run_command <- function(database, ...)
{
    UseMethod("run_command")
}


#' @export
run_command.kusto_database_endpoint <- function(database, command, ...)
{
    server <- database$server
    user <- database$user
    password <- database$pwd

    # token can be a string or an object of class AzureRMR::AzureToken
    token <- if(AzureRMR::is_azure_token(database$token))
        database$token$credentials$access_token
    else if(is.character(database$token))
        database$token
    else stop("Invalid authentication token in database endpoint", call.=FALSE)

    uri <- paste0(server, "/v1/rest/mgmt")
    parse_command_result(call_kusto(token, user, password, uri, database$database, command, ...))
}


call_kusto <- function(token=NULL, user=NULL, password=NULL, uri, db, qry_cmd,
    http_status_handler=c("stop", "warn", "message", "pass"))
{
    body <- list(
        properties=list(Options=list(queryconsistency="weakconsistency")),
        csl=qry_cmd
    )
    if(!is.null(db))
        body <- c(body, db=db)

    # if this is an AzureToken object, refresh if necessary
    if(AzureRMR::is_azure_token(token) && !token$validate())
    {
        message("Access token has expired or is no longer valid; refreshing")
        token$refresh()
    }

    auth_str <- if(!is.null(token))
        paste("Bearer", token)
    else if(!is.null(user) && !is.null(password))
        paste("Basic", openssl::base64_encode(paste(user, password, sep=":")))
    else stop("Must provide authentication details")

    res <- httr::POST(uri, httr::add_headers(Authorization=auth_str), body=body, encode="json")
    
    http_status_handler <- match.arg(http_status_handler)
    if(http_status_handler == "pass")
        return(res)

    cont <- httr::content(res, simplifyVector=TRUE)
    handler <- get(paste0(http_status_handler, "_for_status"), getNamespace("httr"))
    handler(res, make_error_message(cont))
    cont$Tables
}


make_error_message <- function(content)
{
    msg <- if(!is.null(content$Message))
        content$Message
    else if(!is.null(content$error))
    {
        err <- content$error
        sprintf("%s\n%s", err$message, err$`@message`)
    }
    else ""
    paste0("complete Kusto operation. Message:\n", sub("\\.$", "", msg))
}


parse_query_result <- function(tables)
{
    # load TOC table
    n <- nrow(tables)
    toc <- convert_types(tables$Rows[[n]], tables$Columns[[n]])
    result_tables <- which(toc$Name == "PrimaryResult")

    res <- Map(convert_types, tables$Rows[result_tables], tables$Columns[result_tables])

    if(length(res) == 1)
        res[[1]]
    else res
}


parse_command_result <- function(tables)
{
    res <- Map(convert_types, tables$Rows, coltypes_df=tables$Columns)

    if(length(res) == 1)
        res[[1]]
    else res    
}


convert_kusto_datatype <- function(column, kusto_type)
{
    switch(kusto_type,
        long=, Int64=bit64::as.integer64(column),
        int=, integer=, Int32=as.integer(column),
        datetime=, DateTime=as.POSIXct(strptime(column, format='%Y-%m-%dT%H:%M:%OSZ', tz='UTC')),
        real=, Double=, Float=as.numeric(column),
        bool=, Boolean=as.logical(column),
        as.character(column)
    )
}


convert_types <- function(df, coltypes_df)
{
    if(is_empty(df))
        return(list())
    df <- as.data.frame(df, stringsAsFactors=FALSE)
    names(df) <- coltypes_df$ColumnName
    df[] <- Map(convert_kusto_datatype, df, coltypes_df$DataType)
    df
}


