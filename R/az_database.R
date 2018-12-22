#' @export
az_data_explorer_database <- R6::R6Class("az_data_explorer_database", inherit=AzureRMR::az_resource,
public=list(

    cluster=NULL,

    initialize=function(..., data_explorer_cluster=self$cluster)
    {
        super$initialize(...)
        self$cluster <- data_explorer_cluster
    },

    add_principal=function(name, role="User", type="User", fqn="", email="", app_id="")
    {
        principal_array <- list(list(
            name=name,
            role=role,
            type=type,
            fqn=fqn,
            email=email,
            appId=app_id
        ))
        self$do_operation("addPrincipals", body=list(value=principal_array), encode="json", http_verb="POST")
    },

    remove_principal=function(name, role="User", type="User", fqn="", email="", app_id="")
    {
        principal_array <- list(list(
            name=name,
            role=role,
            type=type,
            fqn=fqn,
            email=email,
            appId=app_id
        ))
        self$do_operation("removePrincipals", body=list(value=principal_array), encode="json", http_verb="POST")
    },

    list_principals=function(database)
    {
        self$do_operation("listPrincipals", http_verb="POST")
    },

    get_ade_database=function(tenant=NULL)
    {
        clus <- ade_cluster(self$cluster$name, self$cluster$location, self$tenant)
        ade_database(clus, database)
    }
))
