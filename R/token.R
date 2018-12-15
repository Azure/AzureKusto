create_ade_token <- function(cluster, tenant, ...)
{
    tenant <- normalize_tenant(tenant)
    
}


get_ade_token <- function(cluster, tenant)
{

}


delete_ade_token <- function(cluster, tenant)
{

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
