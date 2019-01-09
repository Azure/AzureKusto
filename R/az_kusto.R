#' @export
az_data_explorer <- R6::R6Class("az_data_explorer", inherit=AzureRMR::az_resource,
public=list(

    start=function()
    {
        self$do_operation("start", http_verb="POST")
    },

    stop=function()
    {
        self$do_operation("stop", http_verb="POST")
    },

    create_database=function(database, retention_period=3650, cache_period=31)
    {
        properties <- list(
            softDeletePeriodInDays=retention_period,
            hotCachePeriodInDays=cache_period
        )

        az_data_explorer_database$new(self$token, self$subscription, self$resource_group,
            type="Microsoft.Kusto/clusters",
            name=file.path(self$name, "databases", database),
            location=self$location,
            properties=properties,
            wait=TRUE,
            data_explorer_cluster=self)
    },

    get_database=function(database)
    {
        name <- file.path(self$name, "databases", database)
        az_data_explorer_database$new(self$token, self$subscription, self$resource_group,
            type="Microsoft.Kusto/clusters",
            name=file.path(self$name, "databases", database),
            data_explorer_cluster=self)
    },

    delete_database=function(database, confirm=TRUE)
    {
        self$get_database_resource(database)$delete(confirm=confirm)
    },

    list_databases=function()
    {
        res <- named_list(self$do_operation("databases")$value)
        names(res) <- basename(names(res))
        lapply(res, function(parms) az_resource$new(self$token, self$subscription, deployed_properties=parms))
    },

    get_default_tenant=function()
    {
        # obtain a default tenant for this cluster:
        # either from the trusted external tenant property, or if blank, from login token
        tenant <- NULL
        if(!is_empty(self$properties$trustedExternalTenants))
            tenant <- self$properties$trustedExternalTenants[[1]]$value
        if(is.null(tenant))
            tenant <- sub("/.+$", "", httr::parse_url(self$token$endpoint$access)$path)
        if(is.null(tenant))
            stop("Unable to find default tenant", call.=FALSE)
        tenant
    },

    get_token=function(tenant=self$get_default_tenant())
    {
        get_ade_token(self$name, normalize_location(self$location), tenant=tenant)
    }
))

