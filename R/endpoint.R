#' @export
kusto_query_endpoint <- function(..., .connection_string=NULL, .azure_token=NULL)
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
        validate_kusto_token(.azure_token)
        props$token <- .azure_token
    }

    # if .azure_token arg not supplied, get it from other properties
    if(is.null(props$token))
        props$token <- find_token(props)
    if(is.null(props$token))
        stop("Only logins with Azure Active Directory are currently supported, unable to acquire token",
             call.=FALSE)
    if(props$token$credentials$resource != props$server)
        warning(sprintf("Mismatch between server (%s) and token resource (%s)",
                        props$token$credentials$resource, props$server))

    if(isTRUE(props$fed))
    {
        warning("Federated logins not yet supported")
        props$fed <- NULL
    }

    class(props) <- "kusto_database_endpoint"
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


# token should be a string or an object of class AzureRMR::AzureToken
validate_kusto_token <- function(token)
{
    if(!(is.character(token) && length(token) == 1) && !AzureRMR::is_azure_token(token))
        stop("Token should be a string or an object of class AzureRMR::AzureToken", call.=FALSE)
}


find_token <- function(properties)
{
    # properties to check for token: usertoken, apptoken, appclientid, appkey
    if(!is_empty(properties$usertoken))
    {
        validate_kusto_token(properties$usertoken)
        return(properties$usertoken)
    }
    if(!is_empty(properties$apptoken))
    {
        validate_kusto_token(properties$apptoken)
        return(properties$apptoken)
    }

    # if no app ID supplied, insert Kusto app ID
    if(is_empty(properties$appclientid))
    {
        message("No app ID supplied; using KustoClient app")
        properties$appclientid <- .kusto_app_id
        auth_type <- "device_code"
    }
    else auth_type <- NULL  # KustoClient needs devicecode, otherwise let get_azure_token choose

    # possibilities for authenticating with AAD:
    # - appid + username + userpwd
    # - appid + appkey
    # - appid only (auth_code/device_code flow)
    token_pwd <- token_user <- NULL

    if(!is_empty(properties$user) && !is_empty(properties$pwd))
    {
        token_pwd <- properties$pwd
        token_user <- properties$user
        auth_type <- "resource_owner"
    }
    else if(!is_empty(properties$appkey))
    {
        token_pwd <- properties$appkey
        auth_type <- "client_credentials"
    }

    return(AzureRMR::get_azure_token(properties$server, tenant=properties$tenantid,
        app=properties$appclientid, password=token_pwd, username=token_user, auth_type=auth_type))
}

