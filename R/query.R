.kusto_app_id <- 'db662dc1-0cfe-4e1c-a843-19a68e65be58'

convert_kusto_datatype <- function(column, kusto_type)
{
    if(kusto_type == 'long')
    {
        return(bit64::as.integer64(column))
    } else if(kusto_type %in% c('int', 'integer'))
    {
        return(as.integer(column))
    } else if(kusto_type == 'datetime')
    {
        return(as.POSIXct(strptime(column, format='%Y-%m-%dT%H:%M:%OSZ', tz='UTC')))
    } else if(kusto_type %in% c('real', 'float'))
    {
        return(as.numeric(column))
    } else if(kusto_type == 'bool')
    {
        return(as.logical(column))
    } else
    {
        return(as.character(column))
    }
}


convert_types <- function(df, coltypes_df)
{
    df <- as.data.frame(df, stringsAsFactors=FALSE)
    names(df) <- coltypes_df$ColumnName
    df[] <- mapply(convert_kusto_datatype, df, coltypes_df$ColumnType, SIMPLIFY=FALSE)
    df
}


run_query <- function(token, server, db, query, ...)
{
    uri <- sprintf("https://%s.kusto.windows.net/v1/rest/query", server)
    call_kusto(token, uri, db, query, ...)
}


run_command <- function(token, server, db, query, ...)
{
    uri <- sprintf("https://%s.kusto.windows.net/v1/rest/mgmt", server)
    call_kusto(token, uri, db, query, ...)
}


call_kusto <- function(token, uri, db, query,
    http_status_handler=c("stop", "warn", "message", "pass"))
{
    body_list <- list(
        db=db,
        properties=list(Options=list(queryconsistency="weakconsistency")),
        csl=query
    )
    auth_str <- paste("Bearer", token)

    res <- httr::POST(uri, httr::add_headers(Authorization=auth_str), body=body_list, encode="json")
    
    http_status_handler <- match.arg(http_status_handler)
    if(http_status_handler == "pass")
        return(res)

    cont <- httr::content(res, simplifyVector=TRUE)
    handler <- get(paste0(http_status_handler, "_for_status"), getNamespace("httr"))
    handler(res, make_error_message(cont))
    parse_tables(cont$Tables)
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


parse_tables <- function(tables)
{
    # load TOC table
    n <- nrow(tables)
    toc <- convert_types(tables$Rows[[n]], tables$Columns[[n]])
    result_tables <- which(toc$Name == "PrimaryResult")

    res <- mapply(convert_types, tables$Rows[result_tables], tables$Columns[result_tables],
        SIMPLIFY=FALSE)

    if(length(res) == 1)
        res[[1]]
    else res
}

    #body_list <- list(
        #db=db,
        #properties=list(Options=list(queryconsistency="weakconsistency")),
        #csl=query
    #)
    #uri <- sprintf("https://%s.kusto.windows.net:443/v1/rest/query", server)
    #auth_str <- paste("Bearer", token)
    #r <- httr::POST(uri, httr::add_headers(Authorization=auth_str),
                    #body=body_list, encode="json")

    #return(r)
    #content <- httr::content(r, simplifyValues=TRUE)

    #if(r$status_code != 200)
    #{
        #if(!is.null(content$Message))
        #{
            #stop(sprintf("HTTP %d %s",
          #r$status_code,
          #content$Message))
        #}
        #else if(!is.null(content$error$innererror))
        #{
            #stop(
                #sprintf(
                  #"HTTP %d %s\n%s\n%s\n%s",
                  #r$status_code,
                  #content$error$code,
                  #content$error$message,
                  #content$error$innererror$code,
                  #content$error$innererror$message
                #)
            #)
        #} else
        #{
            #stop(
                #sprintf(
                  #"HTTP %d %s\n%s",
                  #r$status_code,
                  #content$error$code,
                  #content$error$message
                #)
            #)
        #}
    #} else if(!is.null(content$Exceptions))
    #{
        #stop(sprintf("HTTP %d %s",
             #r$status_code,
             #content$Exceptions[[1]]))
    #}

    #table <- content$Tables[[1]]
    #colnames <- sapply(table$Columns, function(x) x$ColumnName)
    #coltypes <- sapply(table$Columns, function(x) x$ColumnType)
    #rows <- table$Rows

    #for(row in 1:length(rows))
    #{
        #rows[[row]]=lapply(rows[[row]], function(x) ifelse(is.null(x), NA, x))
    #}

    #rows <- t(sapply(rows, function(x) unlist(x)))
    #df <- data.frame(rows, stringsAsFactors=FALSE)
    #names(df) <- colnames
    #convert_types(df, coltypes)
#}
