context("Building Query Parameters")

test_that("Query parameter types can be assigned appropriately",
{
    params <- list(foo="hi",
        bar=1L,
        baz=FALSE,
        quux=1.1,
        dt=as.Date('2019-01-01'))
    types <- AzureKusto:::build_param_list(params)
    expect_equal(
        types,
        "(foo:string, bar:int, baz:bool, quux:real, dt:datetime)")
})

test_that("build_request_body without params gives expected result",
{
    body <- AzureKusto:::build_request_body("Samples", "StormEvents | take 5")
    expect_null(body$properties$Parameters)
    expect_equal(body$db, "Samples")
    expect_equal(body$csl, "StormEvents | take 5")
})

test_that("build_request_body with params gives expected result",
{
    params <- list(
        foo="hi",
        bar=1L,
        baz=FALSE,
        quux=1.1
    )
    body <- AzureKusto:::build_request_body("Samples",
                               "MyFunction(foo, bar, baz, quux)",
                               query_parameters=params)
    expect_equal(body$properties$Parameters$foo, "hi")
    expect_equal(body$db, "Samples")
    expect_equal(body$csl, "declare query_parameters (foo:string, bar:int, baz:bool, quux:real) ; MyFunction(foo, bar, baz, quux)")
})
