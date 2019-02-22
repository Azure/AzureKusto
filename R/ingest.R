#' Ingestion functions for Kusto
#'
#' @param database A Kusto database endpoint object, created with [kusto_database_endpoint].
#' @param src The source data. This can be either a data frame, local filename, or URL.
#' @param dest_table The name of the destination table.
#' @param method For local ingestion, the method to use. See 'Details' below.
#' @param staging_container For local ingestion, an Azure storage container to use for staging the dataset. This can be an object of class either [AzureStor::blob_container] or [AzureStor::adls_filesystem]. Only used if `method="indirect"`.
#' @param ingestion_token For local ingestion, an Azure Active Directory authentication token for the cluster ingestion endpoint. Only used if `method="streaming"`.
#' @param http_status_handler For local ingestion, how to handle HTTP conditions >= 300. Defaults to "stop"; alternatives are "warn", "message" and "pass". The last option will pass through the raw response object from the server unchanged, regardless of the status code. This is mostly useful for debugging purposes, or if you want to see what the Kusto REST API does. Only used if `method="streaming"`.
#' @param key,token,sas Authentication arguments for the Azure storage ingestion methods. If multiple arguments are supplied, a key takes priority over a token, which takes priority over a SAS. Note that these arguments are for authenticating with the Azure _storage account_, as opposed to Kusto itself.
#' @param async For the URL ingestion functions, whether to do the ingestion asychronously. If TRUE, the function will return immediately while the server handles the operation in the background.
#' @param ... Named arguments to be treated as ingestion parameters.
#'
#' @details
#' There are up to 3 possible ways to ingest a local dataset, specified by the `method` argument.
#' - `method="indirect"`: The data is uploaded to blob storage, and then ingested from there. This is the default if the AzureStor package is present.
#' - `method="streaming"`: The data is uploaded to the cluster ingestion endpoint. This is the default if the AzureStor package is not present, however be aware that currently (as of February 2019) streaming ingestion is in beta and has to be enabled for a cluster by filing a support ticket.
#' - `method="inline"`: The data is embedded into the command text itself. This is only recommended for testing purposes, or small datasets.
#'
#' @rdname ingest
#' @export
ingest_local <- function(database, src, dest_table, method=NULL, staging_container=NULL,
    ingestion_token=database$token, http_status_handler="stop", ...)
{
    AzureStor <- requireNamespace("AzureStor", quietly=TRUE)
    if(is.null(method))
        method <- if(AzureStor) "indirect" else "streaming"

    switch(as.character(method),
        indirect=
            ingest_indirect(database, src, dest_table, staging_container, ...),
        streaming=
            ingest_streaming(database, src, dest_table, ingestion_token, http_status_handler, ...),
        inline=
            ingest_inline(database, src, dest_table, ...),
        stop("Bad ingestion method argument", call.=FALSE)
    )
}


#' @rdname ingest
#' @export
ingest_url <- function(database, src, dest_table, async=FALSE, ...)
{
    prop_list <- get_ingestion_properties(...)

    cmd <- paste(".ingest",
        if(async) "async" else NULL,
        "into table",
        escape(ident(dest_table)),
        "(", obfuscate_string(src), ")",
        prop_list)

    run_query(database, cmd)
}


#' @rdname ingest
#' @export
ingest_blob <- function(database, src, dest_table, async=FALSE, key=NULL, token=NULL, sas=NULL, ...)
{
    if(!is.null(key))
        src <- paste0(src, ";", key)
    else if(!is.null(token))
        stop("Kusto does not support authenticating to blob storage with a token", call.=FALSE)
    else if(!is.null(sas))
        src <- paste0(src, "?", sas)

    ingest_url(database, src, dest_table, async, ...)
}


#' @rdname ingest
#' @export
ingest_adls2 <- function(database, src, dest_table, async=FALSE, key=NULL, token=NULL, sas=NULL, ...)
{
    # convert https URI into abfss for Kusto
    src_uri <- httr::parse_url(src)
    if(grepl("^http", src_uri$scheme))
    {
        message("ADLSgen2 URIs should be specified as 'abfss://filesystem@host/path/file'")
        src_uri$scheme <- "abfss"
        src_uri$username <- sub("/.+$", "", src_uri$path)
        src_uri$path <- sub("^[^/]+/", "", src_uri$path)
        src <- httr::build_url(src_uri)
    }
    else if(src_uri$scheme == "adl")
        stop("ADLSgen2 URIs do not use the adl: scheme; did you mean to call ingest_adls1()?", call.=FALSE)

    if(!is.null(key))
        src <- paste0(src, ";", key)
    else if(!is.null(token))
        src <- paste0(src, ";token=", validate_token(token))
    else if(!is.null(sas))
        stop("ADLSgen2 does not support use of shared access signatures", call.=FALSE)
    else src <- paste0(src, ";impersonate")

    ingest_url(database, src, dest_table, async, ...)
}


#' @rdname ingest
#' @export
ingest_adls1 <- function(database, src, dest_table, async=FALSE, key=NULL, token=NULL, sas=NULL, ...)
{
    # convert https URI into adl for Kusto
    src_uri <- httr::parse_url(src)
    if(grepl("^http", src_uri$scheme))
    {
        warning("ADLSgen1 URIs should use the adl: scheme; did you mean to call ingest_adls2()?")
        src_uri$scheme <- "adl"
        src <- httr::build_url(src_uri)
    }
    else if(src_uri$scheme == "abfss")
        stop("ADLSgen1 URIs do not use the abfss: scheme; did you mean to call ingest_adls2()?", call.=FALSE)
    
    if(!is.null(key))
        stop("ADLSgen1 does not use shared keys; did you mean to call ingest_adls2()?", call.=FALSE)
    else if(!is.null(token))
        src <- paste0(src, ";token=", validate_token(token))
    else if(!is.null(sas))
        stop("ADLSgen1 does not support use of shared access signatures", call.=FALSE)

    ingest_url(database, src, dest_table, async, ...)
}


ingest_streaming <- function(database, src, dest_table, ingestion_token=database$token,
    http_status_handler=c("stop", "warn", "message", "pass"), ...)
{
    opts <- list(...)

    if(is.data.frame(src))
    {
        con <- textConnection(NULL, "w")
        on.exit(close(con))
        utils::write.table(src, con, row.names=FALSE, col.names=FALSE, sep=",")
        body <- textConnectionValue(con)
        opts <- utils::modifyList(opts, list(streamFormat="Csv"))
    }
    else body <- readLines(src)

    ingest_uri <- httr::parse_url(database$server)
    ingest_uri$path <- file.path("v1/rest/ingest", database$database, dest_table)
    ingest_uri$query <- opts

    headers <- httr::add_headers(Authorization=paste("Bearer", validate_token(ingestion_token)))

    res <- httr::POST(ingest_uri, headers, body=body, encode="raw")

    http_status_handler <- match.arg(http_status_handler)
    if(http_status_handler == "pass")
        return(res)

    cont <- httr::content(res, simplifyVector=TRUE)
    handler <- get(paste0(http_status_handler, "_for_status"), getNamespace("httr"))
    handler(res, make_error_message(cont))
    cont
}


ingest_indirect <- function(database, src, dest_table, staging_container=NULL, ...)
{
    if(!requireNamespace("AzureStor", quietly=TRUE))
        stop("AzureStor package must be installed to do indirect ingestion", call.=FALSE)

    opts <- utils::modifyList(list(...), list(
        key=staging_container$endpoint$key,
        token=staging_container$endpoint$token,
        sas=staging_container$endpoint$sas))

    if(is.data.frame(src))
    {
        utils::write.table(src, textConnection("con", "w"), row.names=FALSE, col.names=FALSE, sep=",")
        src <- textConnection(con, "r")
        opts <- utils::modifyList(opts, list(streamFormat="Csv"))
    }

    if(inherits(staging_container, "blob_container"))
    {
        uploadfunc <- get("upload_blob", getNamespace("AzureStor"))
        uploadfunc(staging_container, src, dest_table)

        url <- httr::parse_url(staging_container$endpoint$url)
        url$path <- file.path(staging_container$name, dest_table)

        do.call(ingest_blob, c(list(database, httr::build_url(url), dest_table), opts))
    }
    else if(inherits(staging_container, "adls_filesystem"))
    {
        uploadfunc <- get("upload_adls_file", getNamespace("AzureStor"))
        uploadfunc(staging_container, src, dest_table)

        url <- httr::parse_url(staging_container$endpoint$url)
        url$scheme <- "abfss"
        url$username <- staging_container$name
        url$path <- dest_table

        do.call(ingest_adls2, c(list(database, httr::build_url(url), dest_table), opts))
    }
    else stop("Unsupported staging container type", call.=FALSE)
}


ingest_inline <- function(database, src, dest_table, ...)
{
    prop_list <- get_ingestion_properties(...)

    if(is.data.frame(src))
    {
        con <- textConnection(NULL, "w")
        on.exit(close(con))
        utils::write.table(src, con, row.names=FALSE, col.names=FALSE, sep=",")
        records <- textConnectionValue(con)
    }
    else records <- readLines(src)

    cmd <- paste0(".ingest inline into table ",
        escape(ident(dest_table)), " ",
        prop_list,
        "<|\n",
        paste0(records, collapse="\n"))

    run_query(database, cmd)
}


obfuscate_string <- function(string)
{
    paste0("h", escape(string))
}


get_ingestion_properties <- function(...)
{
    props <- list(...)
    if(is_empty(props))
        return(NULL)

    prop_list <- mapply(function(name, value)
    {
        paste(name, escape(value), sep="=")
    }, names(props), props)

    paste("with (", paste(prop_list, collapse=", "), ")")
}


