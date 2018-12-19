#' @export
run_query <- function(token, server, db, query, ...)
{
    uri <- sprintf("https://%s.kusto.windows.net/v1/rest/query", server)
    parse_query_result(call_kusto(token, uri, db, query, ...))
}


#' @export
run_command <- function(token, server, db, command, ...)
{
    uri <- sprintf("https://%s.kusto.windows.net/v1/rest/mgmt", server)
    parse_command_result(call_kusto(token, uri, db, command, ...))
}


call_kusto <- function(token, uri, db, qry_cmd,
    http_status_handler=c("stop", "warn", "message", "pass"))
{
    body <- list(
        db=db,
        properties=list(Options=list(queryconsistency="weakconsistency")),
        csl=qry_cmd
    )
    auth_str <- paste("Bearer", token)

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
    paste0("complete Data Explorer operation. Message:\n", msg)
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
    res <- lapply(tables$Rows, convert_types, coltypes_df=tables$Columns[[1]])

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
        real=, float=as.numeric(column),
        bool=, Boolean=as.logical(column),
        as.character(column)
    )
}


convert_types <- function(df, coltypes_df)
{
    df <- as.data.frame(df, stringsAsFactors=FALSE)
    names(df) <- coltypes_df$ColumnName
    df[] <- Map(convert_kusto_datatype, df, coltypes_df$DataType)
    df
}

