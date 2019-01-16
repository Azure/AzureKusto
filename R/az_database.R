#' @export
az_kusto_database <- R6::R6Class("az_kusto_database", inherit=AzureRMR::az_resource,
public=list(

    cluster=NULL,

    initialize=function(..., kusto_cluster=self$cluster)
    {
        super$initialize(...)
        self$cluster <- kusto_cluster
    },

    delete=function(..., confirm=TRUE)
    {
        if(confirm && interactive())
        {
            yn <- readline(paste0("Do you really want to delete the Kusto database '",
                basename(self$name), "'? (y/N) "))
            if(tolower(substr(yn, 1, 1)) != "y")
                return(invisible(NULL))
        }
        super$delete(..., confirm=FALSE)
    },

    add_principals=function(name, role="User", type="User", fqn="", email="", app_id="")
    {
        principals <- data.frame(
            name=name,
            role=role,
            type=type,
            fqn=fqn,
            email=email,
            appId=app_id,
            stringsAsFactors=FALSE
        )
        val <- self$
            do_operation("addPrincipals", body=list(value=principals), encode="json", http_verb="POST")$
            value
        do.call(rbind, lapply(val, as.data.frame, stringsAsFactors=FALSE))
    },

    remove_principals=function(name, role="User", type="User", fqn="", email="", app_id="")
    {
        principals <- data.frame(
            name=name,
            role=role,
            type=type,
            fqn=fqn,
            email=email,
            appId=app_id,
            stringsAsFactors=FALSE
        )
        val <- self$
            do_operation("removePrincipals", body=list(value=principals), encode="json", http_verb="POST")$
            value
        do.call(rbind, lapply(val, as.data.frame, stringsAsFactors=FALSE))
    },

    list_principals=function(database)
    {
        val <- self$do_operation("listPrincipals", http_verb="POST")$value
        do.call(rbind, lapply(val, as.data.frame, stringsAsFactors=FALSE))
    },

    get_query_endpoint=function(tenant=NULL, user=NULL, pwd=NULL)
    {
        # basicauth not (yet) exposed for customer-created clusters
        if(!is.null(user) && !is.null(pwd))
            stop("Basic user authentication not supported")

        if(is.null(tenant))
            tenant <- self$cluster$get_default_tenant()

        token <- self$cluster$get_aad_token(tenant)
        server <- self$cluster$properties$queryUri
        database <- basename(self$name)
        kusto_query_endpoint(server=server, database=database, tenantid=tenant, .azure_token=token)
    }
))


#' @export
is_kusto_database <- function(x)
{
    R6::is.R6(x) && inherits(x, "az_kusto_database")
}
