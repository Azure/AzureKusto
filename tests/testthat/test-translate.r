context("translate")
library(dplyr)

tbl_iris <- tibble::as.tibble(iris)
names(tbl_iris) <- c("SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species")
tbl_iris <- tbl_kusto_abstract(tbl_iris, "iris", src = simulate_kusto())

test_that("select is translated to project",
{
    q <- tbl_iris %>%
        select(Species, SepalLength) %>%
        show_query()

    expect_equal(q, kql("database('local_df').iris\n| project Species, SepalLength"))
})

test_that("distinct is translated to distinct",
{
    q <- tbl_iris %>%
        distinct(Species, SepalLength) %>%
        show_query()

    expect_equal(q, kql("database('local_df').iris\n| distinct Species, SepalLength"))
})

test_that("kql_infix formats correctly",
{
    fn <- kql_infix("==")
    expr <- fn(translate_kql(foo), translate_kql(bar))
    expect_equal(expr, kql("foo == bar"))
})

test_that("kql_prefix formats correctly",
{
    fn <- kql_prefix("sum")
    expr <- fn(translate_kql(foo), translate_kql(bar), translate_kql(baz))
    expect_equal(expr, kql("sum(foo, bar, baz)"))
})

test_that("filter is translated to where with a single expression",
{
    q <- tbl_iris %>%
        filter(Species == "setosa")

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| where Species == 'setosa'"))
})

test_that("multiple arguments to filter() become multiple where clauses",
{
    q <- tbl_iris %>%
        filter(Species == "setosa", SepalLength > 4.1)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| where Species == 'setosa'\n| where SepalLength > 4.1"))
})

test_that("filter errors on missing symbols",
{
    q <- tbl_iris %>%
        filter(Speciess == "setosa")

    expect_error(show_query(q), "Unknown column `Speciess` ")
    #expect_error(show_query(q), "object 'Speciess' not found")
})

test_that("select and filter can be combined",
{
    q <- tbl_iris %>%
        filter(Species == "setosa") %>%
        select(Species, SepalLength)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| where Species == 'setosa'\n| project Species, SepalLength"))
})

test_that("select errors on column after selected away",
{
    q <- tbl_iris %>%
        select(Species) %>%
        select(SepalLength)

    expect_error(show_query(q), "object 'SepalLength' not found")
})

test_that("mutate translates to extend",
{
    q <- tbl_iris %>%
        mutate(Species2 = Species)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| extend Species2 = Species"))
})

test_that("multiple arguments to mutate() become multiple extend clauses",
{
    q <- tbl_iris %>%
        mutate(Species2 = Species, Species3 = Species2, Foo = 1 + 2)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| extend Species2 = Species\n| extend Species3 = Species2\n| extend Foo = 1 + 2"))
})

test_that("sum() translated correctly",
{
    expect_equal(as.character(translate_kql(MeanSepalLength = mean(SepalLength, na.rm = TRUE))),
                 "avg(SepalLength)"
                 )
})

test_that("arrange() generates order by ",
{
    q <- tbl_iris %>%
        arrange(Species, desc(SepalLength))

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| order by Species asc, SepalLength desc"))
})

test_that("group_by() followed by summarize() generates summarize clause",
{

    q <- tbl_iris %>%
        group_by(Species) %>%
        summarize(MaxSepalLength = max(SepalLength, na.rm = TRUE))

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| summarize MaxSepalLength = max(SepalLength) by Species"))
})

test_that("group_by() followed by ungroup() followed by summarize() generates summarize clause",
{

    q <- tbl_iris %>%
        group_by(Species) %>%
        summarize(MaxSepalLength = max(SepalLength, na.rm = TRUE)) %>%
        ungroup() %>%
        summarize(MeanOfMaxSepalLength = mean(MaxSepalLength, na.rm = TRUE))

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| summarize MaxSepalLength = max(SepalLength) by Species\n| summarize MeanOfMaxSepalLength = avg(MaxSepalLength)"))
})

test_that("group_by() followed by mutate() partitions the mutation by the grouping variables",
{

    q <- tbl_iris %>%
        group_by(Species) %>%
        mutate(SpeciesMaxSepalLength = max(SepalLength, na.rm = TRUE))

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| as tmp | join kind=leftouter (tmp | summarize SpeciesMaxSepalLength = max(SepalLength) by Species) on Species\n| project SepalLength, SepalWidth, PetalLength, PetalWidth, Species, SpeciesMaxSepalLength"))
})

test_that("mutate() with an agg function and no group_by() groups by all other columns",
{
    q <- tbl_iris %>%
        mutate(MaxSepalLength = max(SepalLength, na.rm = TRUE))

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| summarize MaxSepalLength = max(SepalLength) by SepalLength, SepalWidth, PetalLength, PetalWidth, Species"))
})

test_that("is_agg works with symbols and strings",
{
    expect_true(is_agg(n))
    expect_true(is_agg("n"))
    expect_false(is_agg(o))
    expect_false(is_agg("o"))
    expect_false(is_agg(`+`))
    expect_false(is_agg(abs))
    expect_false(is_agg(TRUE))
})

test_that("rename() renames variables",
{
    q <- tbl_iris %>%
        rename(Species2 = Species, SepalLength2 = SepalLength)

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| project-rename Species2 = Species, SepalLength2 = SepalLength"))
})

test_that("rename() errors when given a nonexistent column",
{
    q <- tbl_iris %>%
        rename(Species2 = Species1)

    expect_error(show_query(q), "object 'Species1' not found")
})

test_that("head(10) translates to take 10",
{
    q <- tbl_iris %>%
        head(10)

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("database('local_df').iris\n| take 10"))
})

test_that("head() translates to take 6 (the default)",
{
    q <- tbl_iris %>%
        head()

    q_str <- q %>% show_query

    expect_equal(q_str, kql("database('local_df').iris\n| take 6"))
})

left <- tbl_iris

right <- iris %>%
    group_by(Species) %>%
    summarize(MaxSepalLength = max(Sepal.Length, na.rm = TRUE))

right2 <- iris %>%
    rename(SepalWidth = Sepal.Width) %>%
    group_by(Species, SepalWidth) %>%
    summarize(MaxSepalLength = max(Sepal.Length, na.rm = TRUE))

right3 <- right2 %>% rename(Species2 = Species, SepalWidth2 = SepalWidth)

right <- tbl_kusto_abstract(right, "iris2", src = simulate_kusto())

right2 <- tbl_kusto_abstract(right2, "iris2", src = simulate_kusto())

right3 <- tbl_kusto_abstract(right3, "iris3", src = simulate_kusto())

test_that("inner_join() on a single column translates correctly",
{

    q <- left %>%
        inner_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("database('local_df').iris\n| join kind = inner (database('local_df').iris2) on Species"))
})

test_that("inner_join() on two columns translates correctly",
{
    q <- left %>%
        inner_join(right2, by = c("Species", "SepalWidth"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("database('local_df').iris\n| join kind = inner (database('local_df').iris2) on Species, SepalWidth"))
})

test_that("inner_join() on one differently named column translates correctly",
{
    q <- left %>%
        inner_join(right3, by = c("Species" = "Species2"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("database('local_df').iris\n| join kind = inner (database('local_df').iris3) on $left.Species == $right.Species2"))
})

test_that("inner_join() on two differently named columns translates correctly",
{

    q <- left %>%
        inner_join(right3, by = c("Species" = "Species2", "SepalWidth" = "SepalWidth2"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("database('local_df').iris\n| join kind = inner (database('local_df').iris3) on $left.Species == $right.Species2, $left.SepalWidth == $right.SepalWidth2"))
})

test_that("left_join() on a single column translates correctly",
{

    q <- left %>%
        left_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("database('local_df').iris\n| join kind = leftouter (database('local_df').iris2) on Species"))
})

test_that("right_join() on a single column translates correctly",
{

    q <- left %>%
        right_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("database('local_df').iris\n| join kind = rightouter (database('local_df').iris2) on Species"))
})

test_that("full_join() on a single column translates correctly",
{

    q <- left %>%
        full_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("database('local_df').iris\n| join kind = fullouter (database('local_df').iris2) on Species"))
})

test_that("semi_join() on a single column translates correctly",
{

    q <- left %>%
        semi_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("database('local_df').iris\n| join kind = leftsemi (database('local_df').iris2) on Species"))
})

test_that("anti_join() on a single column translates correctly",
{

    q <- left %>%
        anti_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("database('local_df').iris\n| join kind = leftanti (database('local_df').iris2) on Species"))
})

test_that("union_all translates correctly",
{
    tbl_iris_2 <- tbl_kusto_abstract(iris, "iris", src=simulate_kusto())

    q <- tbl_iris %>%
        union_all(tbl_iris_2)

    q_str <- show_query(q)

    expect_equal(q_str, kql("database('local_df').iris\n| union kind = outer (database('local_df').iris)"))

})
