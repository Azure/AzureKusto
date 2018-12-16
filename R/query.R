.kusto_app_id <- 'db662dc1-0cfe-4e1c-a843-19a68e65be58'

convert_kusto_datatype <- function(column, kusto_type)
{
    if(kusto_type == 'long')
    {
        return(bit64::as.integer64(column))
    } else if(kusto_type == 'integer')
    {
        return(as.integer(column))
    } else if(kusto_type == 'datetime')
    {
        return(as.POSIXct(strptime(column, format='%Y-%m-%dT%H:%M:%OSZ', tz='UTC')))
    } else if(kusto_type == 'real')
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


convert_types <- function(df, coltypes)
{
    df[] <- mapply(convert_kusto_datatype, df, coltypes)
    df
}


execute_query <- function(token, server, db, query)
{
    body_list <- list(
        db=db,
        properties="{\"Options\":{\"queryconsistency\":\"weakconsistency\"}}",
        csl=query
    )
    body_json=jsonlite::toJSON(body_list, auto_unbox=TRUE)
    uri <- sprintf("https://%s.kusto.windows.net:443/v1/rest/query", server)
    auth_str <- paste("Bearer", token)
    r <- httr::POST(
        uri,
        httr::content_type_json(),
        body=body_json,
        httr::add_headers(
          Authorization=auth_str
        )
    )
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
