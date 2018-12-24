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

    create_database_resource=function(database, retention_period=3650, cache_period=31)
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

    get_database_resource=function(database)
    {
        name <- file.path(self$name, "databases", database)
        az_data_explorer_database$new(self$token, self$subscription, self$resource_group,
            type="Microsoft.Kusto/clusters",
            name=file.path(self$name, "databases", database),
            data_explorer_cluster=self)
    },

    delete_database_resource=function(database, confirm=TRUE)
    {
        self$get_database_resource(database)$delete(confirm=confirm)
    },

    list_database_resources=function()
    {
        res <- named_list(self$do_operation("databases")$value)
        names(res) <- basename(names(res))
        lapply(res, function(parms) az_resource$new(self$token, self$subscription, deployed_properties=parms))
    },

    get_cluster_endpoint=function(tenant=NULL)
    {
        # step through possibilities for setting tenant:
        # 1. via argument
        # 2. cluster trusted external tenant
        # 3. from login token
        if(is.null(tenant) && !is_empty(self$properties$trustedExternalTenants))
            tenant <- self$properties$trustedExternalTenants[[1]]$value
        if(is.null(tenant))
            tenant <- sub("/.+$", "", httr::parse_url(self$token$endpoint$access)$path)
        if(is.null(tenant))
            stop("Must provide a tenant", call.=FALSE)

        ade_cluster_endpoint(self$name, normalize_location(self$location), tenant=tenant)
    }
))

