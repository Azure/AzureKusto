#' @export
ade_query_endpoint <- function(..., .connection_string=NULL, .azure_token=NULL)
{
    props <- list(...)
    names(props) <- tolower(names(props))

    if(!is.null(.connection_string))
    {
        # simplified connection string handling: ignores quoting issues
        conn_props <- strsplit(.connection_string, ";")[[1]]
        names(conn_props) <- tolower(sapply(conn_props, function(x) sub("[ ]*=.+$", "", x)))
        conn_props <- lapply(conn_props, function(x) sub("^[^=]+=[ ]*", "", x))

        props <- utils::modifyList(props, conn_props)
    }

    # fix all property names to a given (sub)set, remove quotes from quoted values
    props <- normalize_properties(props)

    if(!is.null(.azure_token))
    {
        if(!(inherits(.azure_token, "AzureToken") && inherits(.azure_token, "R6")))
            stop(".azure_token should be an object of class AzureRMR::AzureToken")

        props$token <- .azure_token
    }
    else
    {
        props$fed <- as.logical(props$fed)
        if(!isTRUE(props$fed))
        {
            warning("Only AAD federated authentication supported at this time", call.=FALSE)
            props$fed <- TRUE
        }

        url <- httr::parse_url(props$server)
        subnames <- strsplit(url$host, ".", fixed=TRUE)[[1]]
        props$token <- get_ade_token(cluster=subnames[1], location=subnames[2], tenant=props$tenantid)
    }

    class(props) <- "ade_database_endpoint"
    props
}


normalize_properties <- function(properties)
{
    # valid property names for a Kusto connection string
    property_list <- list(
        # general properties
        traceclientversion="traceclientversion",
        server=c("server", "addr", "address", "network address", "datasource"),
        database=c("database", "initialcatalog"),
        queryconsistency="queryconsistency",

        # userauth properties -- DSTS not yet supported
        fed=c("fed", "federated security", "federated", "aadfed"),
        pwd=c("pwd", "password"),
        user=c("user", "uid", "userid"),
        traceusername="traceusername",
        usertoken=c("usertoken", "usrtoken"),

        # appauth properties -- cert, DSTS not yet supported
        appclientid=c("appclientid", "applicationclientid"),
        appkey=c("appkey", "applicationkey"),
        traceappname="traceappname",
        apptoken=c("apptoken", "applicationtoken"),
        tenantid=c("tenantid", "authority")
    )

    normalize_name <- function(x)
    {
        for(i in seq_along(property_list))
        {
            if(x %in% property_list[[i]])
                return(names(property_list)[i])
        }
        stop("Invalid/unsupported property name: ", x, call.=FALSE)
    }

    strip_quotes <- function(x)
    {
        sub("^['\"](.+)['\"]$", "\\1", x)
    }

    names(properties) <- sapply(names(properties), normalize_name)
    lapply(properties, strip_quotes)
}
