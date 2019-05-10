context("dplyr")

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
    skip("dplyr integration tests skipped: server info not set")

server <- sprintf("https://%s.%s.kusto.windows.net", srvname, srvloc)

db <- kusto_database_endpoint(server=server, database=dbname, tenantid=tenant,
    appclientid=app, appkey=password)

ir <- tbl_kusto(db, "iris")


test_that("collect works",
{
    expect_is(dplyr::collect(ir), "tbl_df")
})

test_that("select works",
{
    out <- dplyr::select(ir, sepal_length, sepal_width)
    expect_identical(ncol(dplyr::collect(out)), 2L)
})

test_that("distinct works",
{
    out <- dplyr::distinct(ir, species)
    expect_identical(length(dplyr::collect(out)$species), 3L)
})

test_that("filter works",
{
    out <- dplyr::filter(ir, species == "setosa")
    expect_true(all(dplyr::collect(out)$species == "setosa"))
})

test_that("filter and distinct work",
{
    out <- dplyr::filter(dplyr::distinct(ir, species), species == "setosa")
    expect_identical(nrow(dplyr::collect(out)), 1L)
})

test_that("mutate works",
{
    out <- dplyr::mutate(ir, sl2=sepal_length*2)
    expect_true({
        out <- dplyr::collect(out)
        all_equal(out$sl2, out$sepal_length*2)
    })
})

test_that("arrange works",
{
    out <- dplyr::arrange(ir, sepal_length)
    expect_true({
        out <- dplyr::collect(out)
        all(out$sepal_length[-1] >= out$sepal_length[-nrow(out)])
    })
})

test_that("summarise works",
{
    out <- dplyr::summarise(ir, m=mean(sepal_length, na.rm=TRUE))
    expect_identical(nrow(dplyr::collect(out)), 1L)

    out <- dplyr::summarise(dplyr::group_by(ir, species), m=mean(sepal_length, na.rm=TRUE))
    expect_identical(nrow(dplyr::collect(out)), 3L)
})

test_that("rename works",
{
    out <- dplyr::rename(ir, sl=sepal_length)
    expect_identical(names(dplyr::collect(out))[1], "sl")
})

test_that("joining works",
{
    spec <- tbl_kusto(db, "species")

    out <- dplyr::left_join(ir, spec, by="species")
    expect_is(dplyr::collect(out), "tbl_df")

    out <- dplyr::right_join(ir, spec, by="species")
    expect_is(dplyr::collect(out), "tbl_df")

    out <- dplyr::inner_join(ir, spec, by="species")
    expect_is(dplyr::collect(out), "tbl_df")

    out <- dplyr::full_join(ir, spec, by="species")
    expect_is(dplyr::collect(out), "tbl_df")

    expect_error(dplyr::union(ir, ir))
    out <- dplyr::union_all(ir, ir)
    expect_is(dplyr::collect(out), "tbl_df")
})

test_that("summarize hinting works",
{
    out <- dplyr::summarise(dplyr::group_by(ir, species), m=mean(sepal_length, na.rm=TRUE),
        .strategy="shuffle")
    expect_identical(nrow(dplyr::collect(out)), 3L)

    out <- dplyr::summarise(dplyr::group_by(ir, species), m=mean(sepal_length, na.rm=TRUE),
        .strategy="shuffle", .shufflekeys="species")
    expect_identical(nrow(dplyr::collect(out)), 3L)

    out <- dplyr::summarise(dplyr::group_by(ir, species), m=mean(sepal_length, na.rm=TRUE),
        .strategy="shuffle", .num_partitions=2)
    expect_identical(nrow(dplyr::collect(out)), 3L)
})

test_that("join hinting works",
{
    spec <- tbl_kusto(db, "species")

    out <- dplyr::left_join(ir, spec, by="species", .strategy="broadcast")
    expect_is(dplyr::collect(out), "tbl_df")

    out <- dplyr::left_join(ir, spec, by="species", .strategy="shuffle")
    expect_is(dplyr::collect(out), "tbl_df")

    out <- dplyr::left_join(ir, spec, by="species", .strategy="shuffle", .shufflekeys="species")
    expect_is(dplyr::collect(out), "tbl_df")

    out <- dplyr::left_join(ir, spec, by="species", .strategy="shuffle", .num_partitions=2)
    expect_is(dplyr::collect(out), "tbl_df")
})

test_that("parameterised queries work",
{
    ir_parm <- tbl_kusto(db, "iris", parm="setosa")

    out <- dplyr::collect(dplyr::filter(ir_parm, species == parm))
    expect_true(inherits(out, "tbl_df") && all(out$species == "setosa") && nrow(out) == 50)

    out <- dplyr::collect(dplyr::mutate(ir_parm, species2=parm))
    expect_true(inherits(out, "tbl_df") && all(out$species2 == "setosa") && nrow(out) == 150)
})

test_that("cross-cluster joins work",
{
    srvname2 <- Sys.getenv("AZ_TEST_KUSTO_SERVER2")
    srvloc2 <- Sys.getenv("AZ_TEST_KUSTO_SERVER2_LOCATION")
    dbname2 <- Sys.getenv("AZ_TEST_KUSTO_DATABASE2")

    if(srvname2 == "" || srvloc2 == "" || dbname2 == "")
        skip("Cross-cluster join tests skipped: server info not set")

    server2 <- sprintf("https://%s.%s.kusto.windows.net", srvname2, srvloc2)

    db2 <- kusto_database_endpoint(server=server2, database=dbname2, tenantid=tenant,
        appclientid=app, appkey=password)

    spec <- tbl_kusto(db2, "species")

    out <- dplyr::collect(dplyr::left_join(ir, spec))
    expect_true(inherits(out, "tbl_df") && nrow(out) == 150)

    out <- dplyr::collect(dplyr::left_join(ir, spec, .remote="left"))
    expect_true(inherits(out, "tbl_df") && nrow(out) == 150)

    out <- dplyr::collect(dplyr::left_join(ir, spec, .remote="right"))
    expect_true(inherits(out, "tbl_df") && nrow(out) == 150)
})

test_that("nest works",
{
    tbl_iris <- tbl_kusto(db, "iris")
    iris_nested <- dplyr::collect(tidyr::nest(tbl_iris, -species))
    expect_equal(3, nrow(iris_nested))
    expect_true(inherits(iris_nested$sepal_length, "list"))
    expect_equal(50, length(iris_nested[[1, 2]]))
})

test_that("unnest works",
{
    tbl_iris <- tbl_kusto(db, "iris")
    iris_nested <- tidyr::nest(tbl_iris, -species)
    iris_unnested <- tidyr::unnest(iris_nested)
    iris_unnested <- dplyr::mutate(iris_unnested,
                                   sepal_length = toreal(sepal_length),
                                   sepal_width = toreal(sepal_width),
                                   petal_length = toreal(petal_length),
                                   petal_width = toreal(petal_width))
    result <- dplyr::collect(iris_unnested)
    expect_equal(150, nrow(result))
    expect_true(inherits(result$sepal_length, "numeric"))
})
