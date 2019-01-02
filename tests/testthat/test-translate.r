context("translate")
library(dplyr)

test_that("select statement is translated to project",
{

    tbl_iris <- tibble::as.tibble(iris)
    names(tbl_iris) <- c("SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species")
    tbl_iris <- tbl(tbl_iris, "iris")
    q <- tbl_iris %>%
        select(Species, SepalLength) %>%
        show_query()

    expect_equal(q, "database(local_df).iris\n| project Species, SepalLength")
})

test_that("distinct statement is translated to distinct",
{

    tbl_iris <- tibble::as.tibble(iris)
    tbl_iris <- tbl(tbl_iris, "iris")
    q <- tbl_iris %>%
        distinct(Species) %>%
        show_query()

    expect_equal(q, "database(local_df).iris\n| distinct Species")
})
