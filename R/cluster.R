# Azure Active Directory app used to talk to Kusto
.kusto_app_id <- 'db662dc1-0cfe-4e1c-a843-19a68e65be58'


#' Obtain an AAD authentication token for the given Kusto cluster and tenant
#' @export
get_kusto_token <- function(cluster, location=NULL, tenant, refresh=TRUE)
{
    tenant <- normalize_tenant(tenant)
    location <- normalize_location(location)
    cluster <- normalize_cluster(cluster, location)
    host <- paste0("https://", cluster, ".kusto.windows.net")

    filename <- file.path(config_dir(), paste(cluster, "-", tenant))
    if(!file.exists(filename))
        return(create_kusto_token(cluster, location, tenant))

    token <- readRDS(filename)
    if(refresh)
    {
        token$refresh()
        saveRDS(token, filename)
    }
    token
}


#' @export
create_kusto_token <- function(cluster, location=NULL, tenant)
{
    tenant <- normalize_tenant(tenant)
    location <- normalize_location(location)
    cluster <- normalize_cluster(cluster, location)
    host <- paste0("https://", cluster, ".kusto.windows.net")

    token <- AzureRMR::get_azure_token(resource_host=host,
        tenant=tenant,
        app=.kusto_app_id,
        auth_type="device_code",
        aad_host="https://login.microsoftonline.com/")

    filename <- file.path(config_dir(), paste(cluster, "-", tenant))
    saveRDS(token, filename)
    token
}


#' @export
delete_kusto_token <- function(cluster, location=NULL, tenant, confirm=TRUE)
{
    tenant <- normalize_tenant(tenant)
    location <- normalize_location(location)
    cluster <- normalize_cluster(cluster, location)
    if(confirm && interactive())
    {
        yn <- readline(paste0("Do you really want to delete the authentication token for Kusto cluster ",
            cluster, "? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }

    filename <- file.path(config_dir(), paste(cluster, "-", tenant))
    file.remove(filename)
    invisible(NULL)
}


#' @export
list_kusto_tokens <- function()
{
    files <- dir(config_dir(), full.names=TRUE)
    objs <- lapply(files, readRDS)
    names(objs) <- basename(files)
    objs
}


normalize_tenant <- function(tenant)
{
    tenant <- tolower(tenant)
    is_guid <- function(x)
    {
        grepl("^[0-9a-f]{32}$", x) ||
        grepl("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", x) ||
        grepl("^\\{[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\}$", x) ||
        grepl("^\\([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\\)$", x)
    }
    if(is_guid(tenant))
        return(tenant)
    if(!grepl("\\.", tenant))
        tenant <- paste(tenant, "onmicrosoft.com", sep=".")
    tenant
}


# Kusto prettifies location eg "West US" instead of "westus", unprettify it to be on the safe side
normalize_location <- function(location)
{
    tolower(gsub(" ", "", location))
}


normalize_cluster <- function(cluster, location=NULL)
{
    if(is.null(location))
    {
        if(!grepl("\\..+", cluster))
            stop("Must supply cluster location")
        return(cluster)
    }
    
    cluster <- tolower(cluster)
    if(!grepl("\\..+", cluster))
        paste0(cluster, ".", location)
    else cluster
}


