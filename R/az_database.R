#' Kusto/Azure Data Explorer database resource class
#'
#' Class representing a Kusto database, exposing methods for working with it.
#'
#' @docType class
#' @section Methods:
#' The following methods are available, in addition to those provided by the [AzureRMR::az_resource] class:
#' - `new(...)`: Initialize a new storage object. See 'Initialization'.
#' - `add_principals(...)`: Add new database principals. See `Principals` below.
#' - `remove_principals(...)`: Remove database principals.
#' - `list_principals()`: Retrieve all database principals, as a data frame.
#' - `get_query_endpoint()`: Get a query endpoint object for interacting with the database.
#' - `get_ingestion_endpoint()`: Get an ingestion endpoint object for interacting with the database.
#'
#' @section Initialization:
#' Initializing a new object of this class can either retrieve an existing Kusto database, or create a new database on the server. Generally, the best way to initialize an object is via the `get_database`, `list_databases()` and `create_database` methods of the [az_kusto] class, which handle the details automatically.
#'
#' @section Principals:
#' This class provides methods for managing the principals of a database.
#'
#' `add_principal` takes the following arguments. It returns a data frame with one row per principal, containing the details for each principal.
#' - `name`: The name of the principal to create.
#' - `role`: The roleo of the principal, for example "Admin" or "User".
#' - `type`: The type of principal, either "User" or "App".
#' - `fqn`: The fully qualified name of the principal, for example "aaduser=username@mydomain" for an Azure Active Directory account. If supplied, the other details will be obtained from this.
#' - `email`: For a user principal, the email address.
#' - `app_id`: For an application principal, the ID.
#'
#' `remove_principal` removes a principal. It takes the same arguments as `add_principal`; if the supplied details do not match the actual details for the principal, it is not removed.
#'
#' @seealso
#' [az_kusto], [kusto_database_endpoint],
#' [create_database], [get_database], [delete_database]
#'
#' [Kusto/Azure Data Explorer documentation](https://docs.microsoft.com/en-us/azure/data-explorer/),
#'
#' @examples
#' \dontrun{
#'
#' # recommended way of retrieving a resource: via a resource group object
#' db <- resgroup$
#'     get_kusto_cluster("mykusto")$
#'     get_database("mydatabase")
#'
#' # list principals
#' db$list_principals()
#'
#' # add a new principal
#' db$add_principal("New User", role="User", fqn="aaduser=username@mydomain")
#'
#' # get the endpoint
#' db$get_database_endpoint(use_integer64=FALSE)
#'
#' }
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

    list_principals=function()
    {
        val <- self$do_operation("listPrincipals", http_verb="POST")$value
        do.call(rbind, lapply(val, as.data.frame, stringsAsFactors=FALSE))
    },

    get_database_endpoint=function(tenant=NULL, user=NULL, pwd=NULL, ...)
    {
        if(is.null(tenant))
            tenant <- self$cluster$get_default_tenant()

        token <- self$cluster$get_query_token(tenant)
        server <- self$cluster$properties$queryUri
        database <- basename(self$name)
        kusto_database_endpoint(server=server, database=database, tenantid=tenant,
            .query_token=token,
            ...)
    }
))


#' @rdname is
#' @export
is_kusto_database <- function(x)
{
    R6::is.R6(x) && inherits(x, "az_kusto_database")
}
