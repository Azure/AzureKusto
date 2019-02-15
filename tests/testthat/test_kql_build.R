context("KQL Build")



# filter ------------------------------------------------------------------

test_that("filter generates simple expressions",
{
    out <- tbl_kusto_abstract(data.frame(x = 1), "foo") %>%
        filter(x > 1L) %>%
        kql_build()

    expect_equal(out$ops[[2]][[1]], kql('where x > 1'))
})

# mutate ------------------------------------------------------------------

test_that("mutate generates simple expressions",
{
    out <- tbl_kusto_abstract(data.frame(x = 1), "foo") %>%
        mutate(y = x + 1L) %>%
        kql_build()

    expect_equal(out$ops[[2]], kql("extend y = x + 1"))
})
