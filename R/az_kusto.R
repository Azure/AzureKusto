#' @export
az_data_explorer <- R6::R6Class("az_data_explorer", inherit=AzureRMR::az_resource,
public=list(

    get_ade_cluster=function()
    {
        clus_name <- sub(".kusto.windows.net$", "", self$properties$queryUri)
        tenant <- sub("/.*$", "", httr::parse_url(self$token$endpoint$access)$path)
        get_ade_cluster(clus_name, tenant)
    }
))


ade_cluster <- R6::R6Class("ade_cluster",

public=list(

    uri=NULL,
    ingest_uri=NULL,
    token=NULL,

    initialize=function(uri, ingest_uri)
    {
        self$uri <- uri
        self$ingest_uri <- ingest_uri
        NULL
    },

    create_database=function() {},
    get_database=function() {},
    delete_database=function() {}
))

