context("Cluster creation")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Resource creation tests skipped: ARM credentials not set")

username <- Sys.getenv("AZ_TEST_KUSTO_USERNAME")
if(username == "")
    skip("Resource creation tests skipped: database user not set")

srvloc <- Sys.getenv("AZ_TEST_KUSTO_NEW_LOCATION")
if(srvloc == "")
    stop("Resource creation tests skipped: no location set")


sub <- AzureRMR::az_rm$new(tenant=tenant, app=app, password=password)$get_subscription(subscription)

# random resource group/server names
rgname <- paste(sample(letters, 20, replace=TRUE), collapse="")
srvname <- paste(sample(letters, 10, replace=TRUE), collapse="")


test_that("Cluster creation works",
{
    expect_false(sub$resource_group_exists(rgname))
    rg <- sub$create_resource_group(rgname, location=srvloc)
    expect_true(sub$resource_group_exists(rgname))

    # this should 10-15 minutes
    cat("Cluster creation started at ", strftime(Sys.time(), "%Y-%m-%d %H:%M:%S", usetz=TRUE), "\n")
    srv <- rg$create_kusto_cluster(srvname, wait=TRUE)
    expect_true(is_kusto_cluster(srv))

    srv <- rg$get_kusto_cluster(srvname)
    expect_true(is_kusto_cluster(srv))

    default_tenant <- srv$get_default_tenant()
    expect_type(default_tenant, "character")
    expect_true(AzureRMR::normalize_tenant(tenant) == default_tenant)

    db <- srv$create_database("db")
    expect_true(is_kusto_database(db))

    dbs <- srv$list_databases()
    expect_true(is.list(dbs) && all(sapply(dbs, is_kusto_database)))

    db2 <- srv$get_database("db")
    expect_true(is_kusto_database(db2))
    expect_identical(db$name, db2$name)

    db$add_principals(username, role="Admin", fqn=sprintf("aaduser=%s", username))

    pr <- db$list_principals()
    expect_is(pr, "data.frame")

    # role mismatch -> principal not removed
    db$remove_principals(username, fqn=sprintf("aaduser=%s", username))

    pr2 <- db$list_principals()
    expect_true(nrow(pr) == nrow(pr2))

    db$remove_principals(username, role="Admin", fqn=sprintf("aaduser=%s", username))

    pr3 <- db$list_principals()
    expect_true(nrow(pr) > nrow(pr3))

    srv$delete_database("db", confirm=FALSE)
})


teardown(
{
    sub$delete_resource_group(rgname, confirm=FALSE)
})


