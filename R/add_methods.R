# documentation is separate from implementation because roxygen still doesn't know how to handle R6

#' Create Kusto/Azure Data Explorer cluster
#'
#' Method for the [AzureRMR::az_resource_group] class.
#'
#' @rdname create_kusto_cluster
#' @name create_kusto_cluster
#' @aliases create_kusto_cluster create_azure_data_explorer
#' @section Usage:
#' ```
#' create_kusto_cluster(name, location,
#'                      node_size="D14_v2", ...)
#' ```
#' @section Arguments:
#' - `name`: The name of the cluster.
#' - `location`: The location/region in which to create the account. Defaults to the resource group location.
#' - `node_size`: The capacity of the nodes in each of the cluster. Defaults to "D14_v2", which should be available in all regions. The availability of other sizes depends on the region the cluster is created in.
#' - ... Other named arguments to pass to the [az_kusto] initialization function.
#'
#' @section Details:
#' This method deploys a new Kusto cluster resource, with parameters given by the arguments.
#'
#' @section Value:
#' An object of class `az_kusto` representing the created cluster.
#'
#' @seealso
#' [get_kusto_cluster], [delete_kusto_cluster], [az_kusto]
#'
#' [Kusto/Azure Data Explorer documentation](https://docs.microsoft.com/en-us/azure/data-explorer/)
#'
#' @examples
#' \dontrun{
#'
#' rg <- AzureRMR::get_azure_login("myaadtenant")$
#'     get_subscription("subscription_id")$
#'     get_resource_group("rgname")
#'
#' # create a new Kusto cluster
#' rg$create_kusto_cluster("mykusto", node_size="L16")
#'
#' }
NULL

#' Get existing Kusto/Azure Data Explorer cluster
#'
#' Method for the [AzureRMR::az_resource_group] class.
#'
#' @rdname get_kusto_cluster
#' @name get_kusto_cluster
#' @aliases get_kusto_cluster get_azure_data_explorer
#' @section Usage:
#' ```
#' get_kusto_cluster(name, location,
#'                      node_size="D14_v2")
#' ```
#' @section Arguments:
#' - `name`: The name of the cluster.
#'
#' @section Details:
#' This method retrieves an existing Kusto cluster resource.
#'
#' @section Value:
#' An object of class `az_kusto` representing the created cluster.
#'
#' @seealso
#' [create_kusto_cluster], [delete_kusto_cluster], [az_kusto]
#'
#' [Kusto/Azure Data Explorer documentation](https://docs.microsoft.com/en-us/azure/data-explorer/)
#'
#' @examples
#' \dontrun{
#'
#' rg <- AzureRMR::get_azure_login("myaadtenant")$
#'     get_subscription("subscription_id")$
#'     get_resource_group("rgname")
#'
#' # get a Kusto cluster
#' rg$get_kusto_cluster("mykusto")
#'
#' }
NULL

#' Delete Kusto/Azure Data Explorer cluster
#'
#' Method for the [AzureRMR::az_resource_group] class.
#'
#' @rdname delete_kusto_cluster
#' @name delete_kusto_cluster
#' @aliases delete_kusto_cluster, delete_azure_data_explorer
#'
#' @section Usage:
#' ```
#' delete_kusto_cluster(name, confirm=TRUE, wait=FALSE)
#' ```
#' @section Arguments:
#' - `name`: The name of the cluster.
#' - `confirm`: Whether to ask for confirmation before deleting.
#' - `wait`: Whether to wait until the deletion is complete.
#'
#' @section Value:
#' NULL on successful deletion.
#'
#' @seealso
#' [create_kusto_cluster], [get_kusto_cluster], [az_kusto]
#'
#' [Kusto/Azure Data Explorer documentation](https://docs.microsoft.com/en-us/azure/data-explorer/)
#'
#' @examples
#' \dontrun{
#'
#' rg <- AzureRMR::az_rm$
#'     new(tenant="myaadtenant.onmicrosoft.com", app="app_id", password="password")$
#'     get_subscription("subscription_id")$
#'     get_resource_group("rgname")
#'
#' # delete a Kusto cluster
#' rg$delete_kusto_cluster("mycluster")
#'
#' }
NULL


add_methods <- function()
{
    ## extending AzureRMR classes

    AzureRMR::az_resource_group$set("public", "create_kusto_cluster", overwrite=TRUE,
    function(name, location=self$location, node_size="D14_v2", ..., wait=TRUE)
    {
        az_kusto$new(self$token, self$subscription, self$name,
            type="Microsoft.Kusto/clusters", name=name, location=location,
            sku=list(name=node_size, tier="Standard"), ..., wait=wait)
    })


    AzureRMR::az_resource_group$set("public", "get_kusto_cluster", overwrite=TRUE,
    function(name)
    {
        az_kusto$new(self$token, self$subscription, self$name,
            type="Microsoft.Kusto/clusters", name=name)
    })


    AzureRMR::az_resource_group$set("public", "delete_kusto_cluster", overwrite=TRUE,
    function(name, confirm=TRUE, wait=FALSE)
    {
        self$get_kusto_cluster(name)$delete(confirm=confirm, wait=wait)
    })
}
