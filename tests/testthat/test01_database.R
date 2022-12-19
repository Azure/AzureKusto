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

if(!interactive())
    skip("Database endpoint tests skipped: must be an interactive session")

rg <- AzureRMR::az_rm$
    new(tenant=tenant, app=app, password=password)$
    get_subscription(subscription)$
    get_resource_group(rgname)


# should only get 2 devicecode prompts here at most
test_that("Resource access functions work",
{
    srv <- rg$get_kusto_cluster(srvname)
    expect_true(is_kusto_cluster(srv))

    expect_true(AzureAuth::is_azure_token(srv$get_query_token()))
    expect_true(AzureAuth::is_azure_token(srv$get_query_token(app=app, password=password)))

    expect_true(AzureAuth::is_azure_token(srv$get_ingestion_token()))
    expect_true(AzureAuth::is_azure_token(srv$get_ingestion_token(app=app, password=password)))

    db <- srv$get_database(dbname)
    expect_true(is_kusto_database(db))

    endp1 <- db$get_database_endpoint()
    expect_is(endp1, "kusto_database_endpoint")

    server <- srv$properties$uri
    endp2 <- kusto_database_endpoint(server=server, database=dbname, tenantid=tenant)
    expect_is(endp2, "kusto_database_endpoint")

    endp3 <- kusto_database_endpoint(server=server, database=dbname,
        .query_token=get_kusto_token(cluster=srvname, location=normalize_location(srv$location), tenant=tenant))
    expect_is(endp3, "kusto_database_endpoint")

    conn_str <- sprintf("server=%s;database=%s;tenantid=%s", server, dbname, tenant)
    endp4 <- kusto_database_endpoint(.connection_string=conn_str)
    expect_is(endp4, "kusto_database_endpoint")

    expect_identical(endp1$token$hash(), endp2$token$hash())
    expect_identical(endp1$token$hash(), endp3$token$hash())
    expect_identical(endp1$token$hash(), endp4$token$hash())

    # using our own app
    endp5 <- kusto_database_endpoint(server=server, database=dbname, tenantid=tenant,
        appclientid=app, appkey=password)
    expect_is(endp5, "kusto_database_endpoint")

    # invalid property
    expect_error(kusto_property_endpoint(badproperty="foo"))

    # unsupported property (change this if/when federated auth is supported)
    expect_warning(kusto_database_endpoint(server=server, database=dbname, tenantid=tenant, fed=TRUE))

    # quote stripping
    endp6 <- kusto_database_endpoint(server=sprintf("'%s'", server), database=dbname, tenantid=tenant)
    expect_identical(endp6$server, endp1$server)

    # connection string type handling (change this if/when federated auth is supported)
    conn_str2 <- sprintf("server=%s;database=%s;tenantid=%s;fed=true", server, dbname, tenant)
    expect_warning(kusto_database_endpoint(.connection_string=conn_str2))
})


