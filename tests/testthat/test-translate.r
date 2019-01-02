context("translate")
library(dplyr)

test_that("select statement with one column is translated to project",
{

    tbl_iris <- tibble::as.tibble(iris)
    tbl_iris <- tbl(tbl_iris, "iris")
    q <- tbl_iris %>%
        select(Species) %>%
        show_query()

    expect_equal(q, "database(local_df).iris\n| select Species")
})

test_that("distinct statement with one column is translated to distinct",
{

    tbl_iris <- tibble::as.tibble(iris)
    tbl_iris <- tbl(tbl_iris, "iris")
    q <- tbl_iris %>%
        distinct(Species) %>%
        show_query()

    expect_equal(q, "database(local_df).iris\n| distinct Species")
})
