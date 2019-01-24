#' Endpoint for communicating with a Kusto database
#'
#' @param ... Named arguments which are the properties for the endpoint object. See 'Details' below for the properties that AzureKusto recognises.
#' @param .connection_string An alternative way of specifying the properties, as a database connection string. Properties supplied here override those in `...` if they overlap.
#' @param .azure_token Optionally, an Azure Active Directory token to authenticate with. If this is supplied, it overrides other tokens specified in `...` or in the connection string.
#' @param .use_integer64 Whether to convert columns with Kusto `long` datatype into 64-bit integers in R, using the bit64 package. If FALSE, represent them as numeric instead.
#'
#' @details
#' This is a list of properties recognised by `kusto_query_endpoint`, and their alternate names. Property names not in this list will generate an error.
#'
#' General properties:
#' - server: The URI of the server, usually of the form 'https://{clustername}.{location}.kusto.windows.net'.
#'   * addr, address, network address, datasource
#' - database: The database.
#'   * initialcatalog
#' - tenantid: The AAD tenant name or ID to authenticate with.
#'   * authority
#' - appclientid: The AAD app/service principal ID
#'   * applicationclientid
#' - traceclientversion: The client version for tracing.
#' - queryconsistency: The level of query consistency. Defaults to "weakconsistency".
#' - response_dynamic_serialization: How to serialize dynamic responses.
#' - response_dynamic_serialization_2: How to serialize dynamic responses.
#' 
#' User authentication properties:
#  - pwd: The user password.
#'   * password
#' - user: The user name.
#'   * uid, userid
#' - traceusername: The user name for tracing.
#' - usertoken: The AAD token for user authentication.
#' - * usertoken, usrtoken
#' - fed: Logical, whether federated authentication is enabled. Currently unsupported; if this is TRUE, `kusto_query_endpoint` will print a warning and ignore it.
#'   * federated security, federated, aadfed
#'
#' App authentication properties:
#' - appkey: The secret key for the app.
#'   * applicationkey
#' - traceappname: The AAD app for tracing.
#' - apptoken: The AAD token for app authentication.
#'   * apptoken, applicationtoken
#'
#' Currently, AzureKusto only supports authentication via Azure Active Directory, and only via app or user credentials. Authenticating with federated logins, an AAD certificate, or with DSTS is planned for the future.
#'
#' The way `kusto_query_endpoint` obtains an AAD token is as follows.
#' 1. If the `.azure_token` argument is supplied, use it.
#' 2. Otherwise, if the `usertoken` property is supplied, use it.
#' 3. Otherwise, if the `apptoken` property is supplied, use it.
#' 4. Otherwise, if the `appclientid` property is supplied, use it to obtain a token:
#'    - With the `user` and `pwd` properties if available
#'    - Or with the `appkey` property if available
#'    - Otherwise do an interactive authentication and ask for the user credentials
#' 5. Otherwise, if no `appclientid` property is supplied, authenticate with the KustoClient app:
#'    - With the `user` and `pwd` properties if available
#'    - Otherwise do an interactive authentication and ask for the user credentials using a device code
#'
#' @return
#' An object of class `kusto_database_endpoint`.
#' @seealso
#' [run_query], [run_command]
#' @export
kusto_query_endpoint <- function(..., .connection_string=NULL, .azure_token=NULL, .use_integer64=FALSE)
{
    props <- list(...)
    names(props) <- tolower(names(props))

    if(!is.null(.connection_string))
    {
        # simplified connection string handling: ignores quoting issues
        conn_props <- strsplit(.connection_string, ";")[[1]]
        names(conn_props) <- tolower(sapply(conn_props, function(x) sub("[ ]*=.+$", "", x)))
        conn_props <- lapply(conn_props, function(x)
        {
            x <- sub("^[^=]+=[ ]*", "", x)
            find_type_from_connstring(x)
        })
        props <- utils::modifyList(props, conn_props)
    }

    # fix all property names to a given (sub)set, remove quotes from quoted values
    props <- normalize_properties(props)

    if(!is.null(.azure_token))
        props$token <- .azure_token

    # if .azure_token arg not supplied, get it from other properties
    if(is.null(props$token))
        props$token <- find_token(props)
    if(is.null(props$token))
        stop("Only logins with Azure Active Directory are currently supported, unable to acquire token",
             call.=FALSE)
    if(props$token$credentials$resource != props$server)
        warning(sprintf("Mismatch between server (%s) and token resource (%s)",
                        props$token$credentials$resource, props$server))

    props$use_integer64 <- .use_integer64

    props <- handle_unsupported(props)

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
        tenantid=c("tenantid", "authority"),
        queryconsistency="queryconsistency",
        response_dynamic_serialization="response_dynamic_serialization",
        response_dynamic_serialization_2="response_dynamic_serialization_2",

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
        apptoken=c("apptoken", "applicationtoken")
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
        if(is.character(x))
            sub("^['\"](.+)['\"]$", "\\1", x)
        else x
    }

    names(properties) <- sapply(names(properties), normalize_name)
    lapply(properties, strip_quotes)
}


# for a property obtained from a connection string, convert to a type other than char if possible
find_type_from_connstring <- function(string)
{
    if(identical(string, "NA"))
        return(NA)
    if(!is.na(x <- suppressWarnings(as.numeric(string)))) # assign inside comparison!
        return(x)
    if(!is.na(l <- as.logical(string)))
        return(l)
    string
}


find_token <- function(properties)
{
    # properties to check for token: usertoken, apptoken, appclientid, appkey
    if(!is_empty(properties$usertoken))
        return(properties$usertoken)

    if(!is_empty(properties$apptoken))
        return(properties$apptoken)

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
    else if(!is_empty(properties$appkey) && properties$appclientid != .kusto_app_id)
    {
        token_pwd <- properties$appkey
        auth_type <- "client_credentials"
    }

    return(AzureRMR::get_azure_token(properties$server, tenant=properties$tenantid,
        app=properties$appclientid, password=token_pwd, username=token_user, auth_type=auth_type))
}


handle_unsupported <- function(props)
{
    if(isTRUE(props$fed))
    {
        warning("Federated logins not yet supported")
        props$fed <- NULL
    }

    if(isTRUE(props$use_integer64) && !requireNamespace("bit64"))
    {
        warning("bit64 package not installed, cannot use 64-bit integers")
        props$use_integer64 <- FALSE
    }

    if(identical(props$response_dynamic_serialization, "json"))
    {
        warning("Serialization of dynamic response to JSON is not yet supported")
        props$response_dynamic_serialization <- NULL
    }

    if(identical(props$response_dynamic_serialization_2, "json"))
    {
        warning("Serialization of dynamic response to JSON is not yet supported")
        props$response_dynamic_serialization_2 <- NULL
    }

    props
}