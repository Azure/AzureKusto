.qry_opt_names <- c(
    "queryconsistency",
    "response_dynamic_serialization",
    "response_dynamic_serialization_2"
)


#' Run a query or command against a Kusto database
#'
#' @param database A Kusto database endpoint object, as returned by `kusto_query_endpoint`.
#' @param query,command A string containing the query or command. Note that database management commands in KQL are distinct from queries.
#' @param ... For `run_query`, named arguments to be used as parameters for a parameterized query.
#'
#' @details
#' These functions are the workhorses of the AzureKusto package. They communicate with the Kusto server and return the query or command results, as data frames.
#'
#' @seealso
#' [kusto_query_endpoint]
#' @rdname query
#' @export
run_query <- function(database, ...)
{
    UseMethod("run_query")
}


#' @rdname query
#' @export
run_query.kusto_database_endpoint <- function(database, query, ...)
{
    query_params <- list(...)
    server <- database$server
    db <- database$database
    token <- database$token
    user <- database$user
    password <- database$pwd

    qry_opts <- database[names(database) %in% .qry_opt_names]

    uri <- paste0(server, "/v1/rest/query")
    body <- build_request_body(db, query, query_options=qry_opts, query_parameters=query_params)
    auth_str <- build_auth_str(token, user, password)
    result <- call_kusto(uri, body, auth_str)
    parse_query_result(result, database$use_integer64)
}


#' @rdname query
#' @export
run_command <- function(database, ...)
{
    UseMethod("run_command")
}


#' @rdname query
#' @export
run_command.kusto_database_endpoint <- function(database, command, ...)
{
    server <- database$server
    db <- database$database
    token <- database$token
    user <- database$user
    password <- database$pwd

    qry_opts <- database[names(database) %in% .qry_opt_names]

    uri <- paste0(server, "/v1/rest/mgmt")
    body <- build_request_body(db, command, query_options=qry_opts)
    auth_str <- build_auth_str(token, user, password)
    result <- call_kusto(uri, body, auth_str)
    parse_command_result(result, database$use_integer64)
}


get_param_types <- function(query_params)
{
    ps <- mapply(function(name, value)
    {
        type <- switch(class(value),
            "logical"="bool",
            "numeric"="real",
            "integer64"="long",
            "integer"="int",
            "Date"="datetime",
            "POSIXct"="datetime",
            "string"
        )
        paste(name, type, sep=":")
    }, names(query_params), query_params)

    paste0("declare query_parameters (", paste(ps, collapse=", "), ");")
}


build_request_body <- function(db, qry_cmd, query_options=list(), query_parameters=list())
{
    default_query_options <- list(
        queryconsistency="weakconsistency",
        response_dynamic_serialization="string",
        response_dynamic_serialization_2="string")

    query_options <- utils::modifyList(default_query_options, query_options)

    body <- list(
        properties=list(Options=query_options),
        csl=qry_cmd
    )
    if(!is.null(db))
        body <- c(body, db=db)

    if(!is_empty(query_parameters))
    {
        body$csl <- paste(get_param_types(query_parameters), body$csl)
        body$properties$Parameters <- query_parameters   
    }

    body
}


build_auth_str <- function(token=NULL, user=NULL, password=NULL)
{
    token <- validate_kusto_token(token)

    auth_str <- if(!is.null(token))
        paste("Bearer", token)
    else if(!is.null(user) && !is.null(password))
        paste("Basic", openssl::base64_encode(paste(user, password, sep=":")))
    else stop("Must provide authentication details")

    auth_str
}


call_kusto <- function(uri, body, auth_str,
                       http_status_handler=c("stop", "warn", "message", "pass"))
{
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


parse_query_result <- function(tables, .use_integer64)
{
    # if raw http response, pass through unchanged  
    if(inherits(tables, "response"))
        return(tables)

    # load TOC table
    n <- nrow(tables)
    toc <- convert_result_types(tables$Rows[[n]], tables$Columns[[n]], .use_integer64)
    result_tables <- which(toc$Name == "PrimaryResult")

    res <- Map(convert_result_types, tables$Rows[result_tables], tables$Columns[result_tables],
               MoreArgs=list(.use_integer64=.use_integer64))

    if(length(res) == 1)
        res[[1]]
    else res
}


parse_command_result <- function(tables, .use_integer64)
{
    # if raw http response, pass through unchanged  
    if(inherits(tables, "response"))
        return(tables)

    res <- Map(convert_result_types, tables$Rows, coltypes_df=tables$Columns,
               MoreArgs=list(.use_integer64=.use_integer64))

    if(length(res) == 1)
        res[[1]]
    else res    
}


convert_kusto_datatype <- function(column, kusto_type, .use_integer64)
{
    switch(kusto_type,
        long=, Int64=
            if(.use_integer64) bit64::as.integer64(column) else as.numeric(column),
        int=, integer=, Int32=
            as.integer(column),
        datetime=, DateTime=
            as.POSIXct(strptime(column, format='%Y-%m-%dT%H:%M:%OSZ', tz='UTC')),
        real=, Double=, Float=
            as.numeric(column),
        bool=, Boolean=
            as.logical(column),
        as.character(column)
    )
}


convert_result_types <- function(df, coltypes_df, .use_integer64)
{
    if(is_empty(df))
        return(list())
    df <- as.data.frame(df, stringsAsFactors=FALSE)
    names(df) <- coltypes_df$ColumnName
    df[] <- Map(convert_kusto_datatype, df, coltypes_df$DataType, MoreArgs=list(.use_integer64=.use_integer64))
    df
}


validate_kusto_token <- function(token)
{
    # token can be a string or an object of class AzureRMR::AzureToken
    if(AzureRMR::is_azure_token(token))
    {
        if(!token$validate()) # refresh if needed
        {
            message("Access token has expired or is no longer valid; refreshing")
            token$refresh()
        }
        token <- token$credentials$access_token
    }
    else if(!is.character(token))
        stop("Invalid authentication token in database endpoint", call.=FALSE)
    token
}
