add_methods <- function()
{
    ## extending AzureRMR classes

    AzureRMR::az_resource_group$set("public", "create_data_explorer", overwrite=TRUE,
    function(name, location=self$location, node_size="D14_v2", ..., wait=TRUE)
    {
        az_data_explorer$new(self$token, self$subscription, self$name,
            type="Microsoft.Kusto/clusters", name=name, location=location,
            sku=list(name=node_size, tier="Standard"), ..., wait=wait)
    })


    AzureRMR::az_resource_group$set("public", "get_data_explorer", overwrite=TRUE,
    function(name)
    {
        az_data_explorer$new(self$token, self$subscription, self$name,
            type="Microsoft.Kusto/clusters", name=name)
    })


    AzureRMR::az_resource_group$set("public", "delete_data_explorer", overwrite=TRUE,
    function(name, confirm=TRUE, wait=FALSE)
    {
        self$get_data_explorer(name)$delete(confirm=confirm, wait=wait)
    })
}
