.kusto_app_id <- 'db662dc1-0cfe-4e1c-a843-19a68e65be58'


#' @export
create_ade_cluster <- function(cluster, tenant, ...)
{
    tenant <- normalize_tenant(tenant)
    host <- paste0("https://", cluster, ".kusto.windows.net")

    token <- AzureRMR::get_azure_token("https://login.microsoftonline.com",
        tenant=tenant,
        app=.kusto_app_id,
        auth_type="device_code",
        resource_host=host)
    if(is.null(token$app$secret))
        token$app$secret <- ""

    out <- list(host=host, token=token)
    class(out) <- "ade_cluster"
    saveRDS(out, file.path(config_dir(), cluster))
    out
}


#' @export
get_ade_cluster <- function(cluster, ..., refresh=TRUE)
{
    file <- file.path(config_dir(), cluster)
    creds_exist <- file.exists(file)
    if(!creds_exist)
        return(create_ade_cluster(cluster, ...))

    clus <- readRDS(file)
    if(refresh)
    {
        clus$token$refresh()
        saveRDS(clus, file)
    }
    clus
}


#' @export
delete_cluster_credentials <- function(cluster, confirm=TRUE)
{
    file <- file.path(config_dir(), cluster)
    if(confirm && interactive())
    {
        yn <- readline(paste0("Do you really want to delete the credentials for Data Explorer cluster ",
            cluster, "? (y/N) "))
        if(tolower(substr(yn, 1, 1)) != "y")
            return(invisible(NULL))
    }
    file.remove(file)
    invisible(NULL)
}


normalize_tenant <- function(tenant)
{
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
