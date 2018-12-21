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
        op <- file.path("databases", database)
        properties <- list(
            softDeletePeriodInDays=retention_period,
            hotCachePeriodInDays=cache_period
        )
        body <- list(
            properties=properties,
            location=self$location
        )

        self$do_operation(op, body=body, encode="json", http_verb="PUT")
    },

    delete_database=function(database, confirm=TRUE)
    {
        if(confirm && interactive())
        {
            yn <- readline(paste0("Do you really want to delete the Data Explorer database ",
                database, "? (y/N) "))
            if(tolower(substr(yn, 1, 1)) != "y")
                return(invisible(NULL))
        }

        op <- file.path("databases", database)
        self$do_operation(op, http_verb="DELETE")
    },

    add_database_principal=function(database, name, role="User", type="User", fqn="", email="", app_id="")
    {
        op <- file.path("databases", database, "addPrincipals")
        principal_array <- list(list(
            name=name,
            role=role,
            type=type,
            fqn=fqn,
            email=email,
            appId=app_id
        ))
        self$do_operation(op, body=list(value=principal_array), encode="json", http_verb="POST")
    },

    remove_database_principal=function(database, name, role="User", type="User", fqn="", email="", app_id="")
    {
        op <- file.path("databases", database, "removePrincipals")
        principal_array <- list(list(
            name=name,
            role=role,
            type=type,
            fqn=fqn,
            email=email,
            appId=app_id
        ))
        self$do_operation(op, body=list(value=principal_array), encode="json", http_verb="POST")
    },

    list_database_principals=function(database)
    {
        op <- file.path("databases", database, "listPrincipals")
        self$do_operation(op, http_verb="POST")
    },

    get_ade_cluster=function(tenant=NULL)
    {
        clus_name <- sub(".kusto.windows.net$", "", httr::parse_url(self$properties$queryUri)$host)
        if(is.null(tenant))
            tenant <- self$properties$trustedExternalTenants[[1]]$value
        if(is.null(tenant))
            stop("Must provide a tenant", call.=FALSE)

        ade_cluster(clus_name, tenant=tenant)
    }
))

