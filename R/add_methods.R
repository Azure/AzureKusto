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
