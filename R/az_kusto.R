#' @export
az_data_explorer <- R6::R6Class("az_data_explorer", inherit=AzureRMR::az_resource,
public=list(

    get_cluster=function(login=TRUE)
    {
        clus <- ade_cluster(self$properties$queryUri, self$properties$dataIngestionUri)
        if(login)
        {
            tenant <- sub("/.*$", "", httr::parse_url(self$token$endpoint$authorize)$path)
            clus$token <- get_ade_token(tenant=tenant, cluster=cluster$uri)
        }
        clus
    },

    resize=function() {}
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

