context("translate")
library(dplyr)

test_that("select is translated to project",
{

    tbl_iris <- tibble::as.tibble(iris)
    names(tbl_iris) <- c("SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species")
    tbl_iris <- tbl(tbl_iris, "iris")
    q <- tbl_iris %>%
        select(Species, SepalLength) %>%
        show_query()

    expect_equal(q, "database(local_df).iris\n| project Species, SepalLength")
})

test_that("distinct is translated to distinct",
{
    tbl_iris <- tibble::as.tibble(iris)
    names(tbl_iris) <- c("SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species")
    tbl_iris <- tbl(tbl_iris, "iris")
    q <- tbl_iris %>%
        distinct(Species, SepalLength) %>%
        show_query()

    expect_equal(q, "database(local_df).iris\n| distinct Species, SepalLength")
})

test_that("infix formats correctly",
{
    fn <- infix("==")
    expr <- fn("foo", "bar")
    expect_equal(expr, "foo == bar")
})

test_that("prefix formats correctly",
{
    fn <- prefix("sum")
    expr <- fn("foo", "bar", "baz")
    expect_equal(expr, "sum(foo, bar, baz)")
})

test_that("filter is translated to where",
{

    tbl_iris <- tibble::as.tibble(iris)
    names(tbl_iris) <- c("SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species")
    tbl_iris <- tbl(tbl_iris, "iris")
    q <- tbl_iris %>%
        filter(Species == "setosa")

    q_str <- q %>%
        show_query()

    expect_equal(q_str, "database(local_df).iris\n| where Species == 'setosa'")
})

test_that("filter errors on missing symbols", {
    tbl_iris <- tibble::as.tibble(iris)
    names(tbl_iris) <- c("SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species")
    tbl_iris <- tbl(tbl_iris, "iris")
    q <- tbl_iris %>%
        filter(Speciess == "setosa")

    expect_error(show_query(q), "object 'Speciess' not found")
})
