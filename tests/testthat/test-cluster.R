context("Kusto Cluster Management")

test_that("normalize_cluster handles clusters with location",
{
    cluster_name <- normalize_cluster("help", normalize_location("westus"))
    expect_equal(cluster_name, "help.westus")
})

test_that("normalize_cluster handles clusters with location inside the cluster name",
{
    cluster_name <- normalize_cluster("help.westus")
    expect_equal(cluster_name, "help.westus")
})

test_that("normalize_cluster handles clusters without location",
{
    cluster_name <- normalize_cluster("help")
    expect_equal(cluster_name, "help")
})
