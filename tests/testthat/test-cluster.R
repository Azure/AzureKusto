context("Kusto Cluster Management")

test_that("normalize_cluster handles clusters with location",
{
    cluster_name <- normalize_cluster("hongkust2", normalize_location("australiasoutheast"))
    expect_equal(cluster_name, "hongkust2.australiasoutheast")
})

test_that("normalize_cluster handles clusters without location",
{
    cluster_name <- normalize_cluster("help")
    expect_equal(cluster_name, "help")
})
