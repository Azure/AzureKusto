# Azure Active Directory app used to talk to Kusto
.kusto_app_id <- 'db662dc1-0cfe-4e1c-a843-19a68e65be58'


#' Manage AAD authentication tokens for Kusto clusters
#'
#' @param server The URI of your Kusto cluster. If not supplied, it is obtained from the `clustername` and `location` arguments.
#' @param clustername The cluster name.
#' @param location The cluster location. Leave this blank for a Microsoft-internal Kusto cluster like "help".
#' @param tenant Your Azure Active Directory (AAD) tenant. Can be a GUID, a name ("myaadtenant") or a fully qualified domain name ("myaadtenant.com").
#' @param hash For `delete_kusto_tenant`, the MD5 hash of the token. This is used to identify the token if provided.
#' @param app The ID of the Azure Active Directory app/service principal to authenticate with. Defaults to the ID of the KustoClient app.
#' @param auth_type The authentication method to use. Can be one of "authorization_code", "device_code", "client_credentials" or "resource_owner". The default is to pick one based on the other arguments.
#' @param ... Other arguments to pass to [AzureRMR::get_azure_token].
#'
#' @details
#' `get_kusto_token` returns an authentication token for the given cluster, caching its value on disk. `delete_kusto_token` deletes a cached token, and `list_kusto_tokens` lists all cached tokens.
#'
#' By default, authentication tokens will be obtained using the main KustoClient Active Directory app. This app can be used to authenticate with any Kusto cluster (assuming, of course, you have the proper credentials).
#'
#' @return
#' `get_kusto_token` returns an object of class AzureRMR::AzureToken representing the authentication token, while `list_kusto_tokens` returns a list of such objects. `delete_azure_token` returns NULL on a successful delete.
#'
#' @seealso
#' [kusto_query_endpoint], [AzureRMR::get_azure_token]
#' @export
get_kusto_token <- function(server=NULL, clustername, location=NULL, tenant, app=.kusto_app_id, auth_type=NULL, ...)
{
    tenant <- AzureRMR::normalize_tenant(tenant)
    if(is.null(server))
    {
        location <- normalize_location(location)
        cluster <- normalize_cluster(clustername, location)
        server <- paste0("https://", cluster, ".kusto.windows.net")
    }

    # KustoClient requires devicecode auth if username not supplied
    if(is.null(auth_type) && app == .kusto_app_id && (!"username" %in% names(list(...))))
        auth_type <- "device_code"

    AzureRMR::get_azure_token(server, tenant, app, auth_type=auth_type, ...)
}


#' @rdname get_kusto_token
#' @export
delete_kusto_token <- function(server=NULL, clustername, location=NULL, tenant, app=.kusto_app_id, auth_type=NULL,
                               ..., hash=NULL, confirm=TRUE)
{
    # use hash if provided
    if(!is.null(hash))
        return(AzureRMR::delete_azure_token(hash=hash, confirm=confirm))

    tenant <- AzureRMR::normalize_tenant(tenant)
    if(is.null(server))
    {
        location <- normalize_location(location)
        cluster <- normalize_cluster(clustername, location)
        server <- paste0("https://", cluster, ".kusto.windows.net")
    }

    # KustoClient requires devicecode auth if username not supplied
    if(is.null(auth_type) && app == .kusto_app_id && (!"username" %in% names(list(...))))
        auth_type <- "device_code"

    AzureRMR::delete_azure_token(server, tenant, app, auth_type=auth_type, confirm=confirm)
}


#' @rdname get_kusto_token
#' @export
list_kusto_tokens <- function()
{
    lst <- AzureRMR::list_azure_tokens()

    is_kusto <- sapply(lst, function(tok)
        grepl("kusto.windows.net", tok$credentials$resource, fixed=TRUE))

    lst[is_kusto]
}


# Kusto prettifies location eg "West US" instead of "westus", unprettify it for URL fiddling purposes
normalize_location <- function(location)
{
    if(is.null(location))
        NULL
    else tolower(gsub(" ", "", location))
}


normalize_cluster <- function(clustername, location=NULL)
{
    if(is.null(location))
        clustername
    else paste(clustername, location, sep=".")
}


