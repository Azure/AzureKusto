context("Databases")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Tests skipped: ARM credentials not set")

# use persistent testing server
rgname <- Sys.getenv("AZ_TEST_KUSTO_SERVER_RG")
srvname <- Sys.getenv("AZ_TEST_KUSTO_SERVER")
dbname <- Sys.getenv("AZ_TEST_KUSTO_DATABASE")
if(rgname == "" || srvname == "" || dbname == "")
    skip("Database endpoint tests skipped: server info not set")

rg <- AzureRMR::az_rm$
    new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)


# should only get one devicecode prompt here at most
test_that("Resource access functions work",
{
    srv <- rg$get_kusto_cluster(srvname)
    expect_true(is_kusto_cluster(srv))

    expect_true(AzureRMR::is_azure_token(srv$get_aad_token()))
    expect_true(AzureRMR::is_azure_token(srv$get_aad_token(app=app, password=password)))

    db <- srv$get_database(dbname)
    expect_true(is_kusto_database(db))

    endp1 <- db$get_query_endpoint()
    expect_is(endp1, "kusto_query_endpoint")

    server <- srv$properties$queryUri
    endp2 <- kusto_query_endpoint(server=server, database=dbname, tenantid=tenant)
    expect_is(endp2, "kusto_query_endpoint")

    endp3 <- kusto_query_endpoint(server=server, database=dbname,
        .azure_token=get_kusto_token(cluster=srvname, location=srv$location, tenant=tenant))
    expect_is(endp3, "kusto_query_endpoint")

    conn_str <- sprintf("server=%s;database=%s;tenantid=%s", server, dbname, tenant)
    endp4 <- kusto_query_endpoint(.connection_string=conn_str)
    expect_is(endp4, "kusto_query_endpoint")

    expect_identical(endp1$token$hash(), endp2$token$hash())
    expect_identical(endp1$token$hash(), endp3$token$hash())
    expect_identical(endp1$token$hash(), endp4$token$hash())

    endp5 <- kusto_query_endpoint(server=server, database=dbname,
        appclientid=app, appkey=password)
    expect_is(endp5, "kusto_query_endpoint")

    # no trailing / on server should trigger warning
    expect_warning(kusto_query_endpoint(
        server=sprintf("https://%s.%s.kusto.windows.net", srvname, srv$location),
        database=dbname,
        .azure_token=endp4$token))

    # invalid property
    expect_error(kusto_property_endpoint(badproperty="foo"))
})


