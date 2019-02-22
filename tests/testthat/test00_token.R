context("AAD tokens")


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


tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")

if(tenant == "" || app == "" || password == "")
    skip("Tests skipped: ARM credentials not set")

# use persistent testing server
srvname <- Sys.getenv("AZ_TEST_KUSTO_SERVER")
srvloc <- Sys.getenv("AZ_TEST_KUSTO_SERVER_LOCATION")
if(srvname == "" || srvloc == "")
    skip("Token acquisition tests skipped: server info not set")

if(!interactive())
    skip("Token acquisition tests skipped: must be an interactive session")

# remove all cached Kusto tokens before testing
lapply(AzureAuth::list_azure_tokens(), function(token)
{
    hash <- token$hash()
    name_suffix <- "kusto\\.windows\\.net"
    if((!is.null(token$resource) && grepl(name_suffix, token$resource)) ||
       (!is.null(token$scope) && grepl(name_suffix, token$scope)) ||
       (!is.null(token$credentials$resource) && grepl(name_suffix, token$credentials$resource)))
        file.remove(file.path(AzureAuth::AzureR_dir(), hash))
})


# should only get one devicecode prompt here
test_that("Obtaining token from KustoClient works",
{
    tok1 <- get_kusto_token(clustername=srvname, location=srvloc, tenant=tenant)
    expect_true(AzureAuth::is_azure_token(tok1))

    srvuri <- sprintf("https://%s.%s.kusto.windows.net", srvname, srvloc)
    tok2 <- get_kusto_token(srvuri, tenant=tenant)
    expect_true(AzureAuth::is_azure_token(tok2))

    expect_identical(tok1$hash(), tok2$hash())
    expect_identical(tok1$hash(), tok2$hash())

    Sys.setenv(AZ_TEST_KUSTO_TOKEN_HASH=tok1$hash())

    expire <- as.numeric(tok1$credentials$expires_on)
    Sys.sleep(5)
    tok1$refresh()
    expect_true(as.numeric(tok1$credentials$expires_on) > expire)

    toks <- list_kusto_tokens()
    expect_true(is.list(toks) && all(sapply(toks, AzureAuth::is_azure_token)))
})


test_that("Obtaining token from own app works",
{
    tok1 <- get_kusto_token(clustername=srvname, location=srvloc, tenant=tenant, app=app, password=password)
    expect_true(AzureAuth::is_azure_token(tok1))

    srvuri <- sprintf("https://%s.%s.kusto.windows.net", srvname, srvloc)
    tok2 <- get_kusto_token(srvuri, tenant=tenant, app=app, password=password)
    expect_true(AzureAuth::is_azure_token(tok2))

    expect_identical(tok1$hash(), tok2$hash())
    expect_identical(tok1$hash(), tok2$hash())
})


# should get one devicecode prompt here
test_that("Obtaining token for common tenant works",
{
    srvuri <- sprintf("https://%s.%s.kusto.windows.net", srvname, srvloc)
    tok <- get_kusto_token(srvuri)
    expect_true(AzureAuth::is_azure_token(tok))
})

Sys.unsetenv("AZ_TEST_KUSTO_TOKEN_HASH")
