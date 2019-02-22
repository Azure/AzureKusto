context("DBI")

tenant <- Sys.getenv("AZ_TEST_TENANT_ID")
app <- Sys.getenv("AZ_TEST_APP_ID")
password <- Sys.getenv("AZ_TEST_PASSWORD")
subscription <- Sys.getenv("AZ_TEST_SUBSCRIPTION")

if(tenant == "" || app == "" || password == "" || subscription == "")
    skip("Tests skipped: ARM credentials not set")

# use persistent testing server
srvname <- Sys.getenv("AZ_TEST_KUSTO_SERVER")
srvloc <- Sys.getenv("AZ_TEST_KUSTO_SERVER_LOCATION")
dbname <- Sys.getenv("AZ_TEST_KUSTO_DATABASE")
if(srvname == "" || srvloc == "" || dbname == "")
    skip("DBI tests skipped: server info not set")

server <- sprintf("https://%s.%s.kusto.windows.net", srvname, srvloc)

if(!requireNamespace("bit64"))
    skip("DBI tests skipped: bit64 package not installed")


test_that("DBI connection works",
{
    drv <- AzureKusto()
    expect_s4_class(drv, "AzureKustoDriver")

    expect_true(DBI::dbCanConnect(drv, host=server, dbname=dbname, tenantid=tenant))
    expect_true(DBI::dbCanConnect(drv, host=server, dbname=dbname, tenantid=tenant,
        appclientid=app, appkey=password))

    dbi <- DBI::dbConnect(drv, host=server, dbname=dbname, tenantid=tenant, appclientid=app, appkey=password)
    expect_s4_class(dbi, "AzureKustoConnection")
    expect_is(dbi@endpoint, "kusto_database_endpoint")
})


test_that("DBI table functions work",
{
    dbi <- DBI::dbConnect(AzureKusto(), host=server, dbname=dbname, tenantid=tenant, appclientid=app, appkey=password)

    expect_is(DBI::dbListTables(dbi), "character")

    expect_true(DBI::dbExistsTable(dbi, "iris"))

    ir <- DBI::dbReadTable(dbi, "iris")
    expect_equal(nrow(ir), 150)
    expect_equal(ncol(ir), 5)

    # non-syntactic names
    expect_false(DBI::dbExistsTable(dbi, "name with spaces"))
    expect_silent(DBI::dbCreateTable(dbi, "name with spaces", mtcars))
    expect_true(DBI::dbExistsTable(dbi, "name with spaces"))
    expect_silent(DBI::dbRemoveTable(dbi, "name with spaces"))

    # don't test row insertion: Kusto has unpredictable lags
})


test_that("DBI query functions work",
{
    dbi <- DBI::dbConnect(AzureKusto(), host=server, dbname=dbname, tenantid=tenant, appclientid=app, appkey=password)

    fields <- DBI::dbListFields(dbi, "iris")
    expect_true(is.character(fields) && length(fields) == 5)

    df <- DBI::dbGetQuery(dbi, "iris")
    expect_is(df, "data.frame")
    expect_equal(nrow(df), 150)
    expect_equal(ncol(df), 5)

    res <- DBI::dbSendQuery(dbi, "iris")
    expect_s4_class(res, "AzureKustoResult")

    df2 <- DBI::dbFetch(res)
    expect_identical(df, df2)

    cols <- DBI::dbColumnInfo(res)
    expect_true(is.data.frame(cols) && nrow(cols) == 5 && ncol(cols) == 2)

    tabs <- DBI::dbExecute(dbi, ".show tables")
    res <- DBI::dbSendStatement(dbi, ".show tables")
    tabs2 <- DBI::dbFetch(res)
    expect_identical(tabs, tabs2)

    expect_error(DBI::dbExecute(dbi, "iris | count"))
})


test_that("integer64 support works",
{
    drv <- AzureKusto()

    dbi1 <- DBI::dbConnect(drv, host=server, dbname=dbname, tenantid=tenant,
        appclientid=app, appkey=password, bigint="numeric")
    expect_s4_class(dbi1, "AzureKustoConnection")

    dbi2 <- DBI::dbConnect(drv, host=server, dbname=dbname, tenantid=tenant,
        appclientid=app, appkey=password, bigint="integer64")
    expect_s4_class(dbi2, "AzureKustoConnection")

    df1 <- DBI::dbGetQuery(dbi1, "iris | count")
    expect_true(is.numeric(df1$Count))

    df2 <- DBI::dbGetQuery(dbi2, "iris | count")
    expect_true(bit64::is.integer64(df2$Count))
})
