#' Kusto/Azure Data Explorer cluster resource class
#'
#' Class representing a Kusto cluster, exposing methods for working with it.
#'
#' @docType class
#' @section Methods:
#' The following methods are available, in addition to those provided by the [AzureRMR::az_resource] class:
#' - `new(...)`: Initialize a new storage object. See 'Initialization'.
#' - `start()`: Start the cluster.
#' - `stop()`: Stop the cluster.
#' - `create_database(...)`: Create a new Kusto database. See `Databases` below.
#' - `get_database(database))`: Get an existing database.
#' - `delete_database(database, confirm=TRUE)`: Delete a database, by default asking for confirmation first.
#' - `list_databases()`: List all databases in this cluster.
#' - `get_default_tenant()`: Retrieve the default tenant to authenticate with this cluster.
#' - `get_query_token(tenant, ...)`: Obtain an authentication token from Azure Active Directory for this cluster's query enpdoint. Accepts further arguments that will be passed to [get_kusto_token].
#' - `get_ingestion_token(tenant, ...)`: Obtain an authentication token for this cluster's ingestion endpoint. Accepts further arguments that will be passed to [get_kusto_token].
#'
#' @section Initialization:
#' Initializing a new object of this class can either retrieve an existing Kusto cluster, or create a new cluster on the host. Generally, the best way to initialize an object is via the `get_kusto_cluster` and `create_kusto_cluster` methods of the [az_resource_group] class, which handle the details automatically.
#'
#' @section Databases:
#' A Kusto cluster can have several databases, which are represented in AzureKusto via [az_kusto_database] R6 objects. The `az_kusto` class provides the `create_database`, `get_database`, `delete_database` and `list_databases` methods for creating, deleting and retrieving databases. It's recommended to use these methods rather than calling `az_kusto_database$new()` directly.
#'
#' `create_database` takes the following arguments. It returns an object of class [az_kusto_database]
#' - `database`: The name of the database to create.
#' - `retention_period`: The retention period of the database, after which data will be soft-deleted.
#' - `cache_period`: The cache period of the database, the length of time for which queries will be cached.
#'
#' `get_database` takes a single argument `database`, the name of the database to retrieve, and returns an object of class `az_kusto_database`. `delete_database` takes the name of the database to delete and returns NULL on a successful deletion. `list_databases` takes no arguments and returns a list of `az_kusto_database` objects, one for each database in the cluster.
#'
#' @seealso
#' [az_kusto_database], [kusto_database_endpoint],
#' [create_kusto_cluster], [get_kusto_cluster], [delete_kusto_cluster],
#' [get_kusto_token]
#'
#' [Kusto/Azure Data Explorer documentation](https://docs.microsoft.com/en-us/azure/data-explorer/),
#'
#' @examples
#' \dontrun{
#'
#' # recommended way of retrieving a resource: via a resource group object
#' kus <- resgroup$get_kusto_cluster("mykusto")
#'
#' # list databases
#' kust$list_databases()
#'
#' # create a new database with a retention period of 6 months
#' kust$create_database("newdb", retention_period=180)
#'
#' # get the default authentication tenant
#' kus$get_default_tenant()
#'
#' # generate an authentication token
#' kust$get_aad_token()
#'
#' }
#' @aliases create_database get_database delete_database list_databases
#' @export
az_kusto <- R6::R6Class("az_kusto", inherit=AzureRMR::az_resource,
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

        az_kusto_database$new(self$token, self$subscription, self$resource_group,
            type="Microsoft.Kusto/clusters",
            name=file.path(self$name, "databases", database),
            location=self$location,
            properties=properties,
            wait=TRUE,
            kusto_cluster=self)
    },

    get_database=function(database)
    {
        name <- file.path(self$name, "databases", database)
        az_kusto_database$new(self$token, self$subscription, self$resource_group,
            type="Microsoft.Kusto/clusters",
            name=file.path(self$name, "databases", database),
            kusto_cluster=self)
    },

    delete_database=function(database, confirm=TRUE)
    {
        self$get_database(database)$delete(confirm=confirm)
    },

    list_databases=function()
    {
        res <- AzureRMR::named_list(self$do_operation("databases")$value)
        names(res) <- basename(names(res))
        lapply(res, function(parms)
            az_kusto_database$new(self$token, self$subscription, deployed_properties=parms))
    },

    get_default_tenant=function()
    {
        # obtain a default tenant for this cluster:
        # either from the trusted external tenant property, or if blank, from login token
        tenant <- NULL
        if(!is_empty(self$properties$trustedExternalTenants))
            tenant <- self$properties$trustedExternalTenants[[1]]$value
        if(is.null(tenant))
            tenant <- self$token$tenant
        if(is.null(tenant))
            stop("Unable to find default tenant", call.=FALSE)
        tenant
    },

    get_query_token=function(tenant=self$get_default_tenant(), ...)
    {
        get_kusto_token(server=self$properties$queryUri, tenant=tenant, ...)
    },

    get_ingestion_token=function(tenant=self$get_default_tenant(), ...)
    {
        get_kusto_token(server=self$properties$dataIngestionUri, tenant=tenant, ...)
    }
))


#' Information functions
#'
#' These functions test whethen an object is of the given class.
#'
#' @param x An R object.
#'
#' @rdname is
#' @export
is_kusto_cluster <- function(x)
{
    R6::is.R6(x) && inherits(x, "az_kusto")
}
