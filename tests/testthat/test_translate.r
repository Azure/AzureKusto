context("translate")


tbl_iris <- tibble::as_tibble(iris)
names(tbl_iris) <- c("SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species")
tbl_iris <- tbl_kusto_abstract(tbl_iris, "iris")
tidyr_version <- as.character(packageVersion("tidyr"))
is_new_tidyr <- compareVersion(tidyr_version, '0.8.3') == 1

test_that("params to a function can be used inside a mutate expressions",
{
    tbl_iris_p <- tibble::as_tibble(iris)
    names(tbl_iris_p) <- c("SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species")
    tbl_iris_p <- tbl_kusto_abstract(tbl_iris, "iris", p="setosa")

    q <- tbl_iris_p %>%
        mutate(Species = p)

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| extend ['Species'] = 'setosa'"))
})

test_that("params to a function can be used inside a filter expressions",
{
    tbl_iris_p <- tibble::as_tibble(iris)
    names(tbl_iris_p) <- c("SepalLength", "SepalWidth", "PetalLength", "PetalWidth", "Species")
    tbl_iris_p <- tbl_kusto_abstract(tbl_iris_p, "iris", p="setosa")

    q <- filter(tbl_iris_p, Species == p)
    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| where ['Species'] == 'setosa'"))
})


test_that("select is translated to project",
{
    q <- tbl_iris %>%
        dplyr::select(Species, SepalLength) %>%
        show_query()

    expect_equal(q, kql("cluster('local_df').database('local_df').['iris']\n| project ['Species'], ['SepalLength']"))
})

test_that("distinct is translated to distinct",
{
    q <- tbl_iris %>%
        dplyr::distinct(Species, SepalLength) %>%
        show_query()

    expect_equal(q, kql("cluster('local_df').database('local_df').['iris']\n| distinct ['Species'], ['SepalLength']"))
})

test_that("kql_infix formats correctly",
{
    fn <- kql_infix("==")
    expr <- fn(translate_kql(foo), translate_kql(bar))
    expect_equal(expr, kql("['foo'] == ['bar']"))
})

test_that("kql_prefix formats correctly",
{
    fn <- kql_prefix("sum")
    expr <- fn(translate_kql(foo), translate_kql(bar), translate_kql(baz))
    expect_equal(expr, kql("sum(['foo'], ['bar'], ['baz'])"))
})

test_that("filter is translated to where with a single expression",
{
    q <- tbl_iris %>%
        dplyr::filter(Species == "setosa")

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| where ['Species'] == 'setosa'"))
})

test_that("multiple arguments to filter() become multiple where clauses",
{
    q <- tbl_iris %>%
        dplyr::filter(Species == "setosa", SepalLength > 4.1)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| where ['Species'] == 'setosa'\n| where ['SepalLength'] > 4.1"))
})

test_that("filter errors on missing symbols",
{
    q <- tbl_iris %>%
        dplyr::filter(Speciess == "setosa")

    expect_error(show_query(q), "Unknown column `Speciess`")
})

test_that("variables from enclosing environment are passed to filter()",
{
    sepal_length_limit <- 2.5
    q <- tbl_iris %>%
        dplyr::filter(SepalLength <= sepal_length_limit)

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| where ['SepalLength'] <= 2.5"))
})

test_that("variables from enclosing environment are passed to mutate()",
{
    sepal_length_limit <- 2.5
    q <- tbl_iris %>%
        dplyr::mutate(SepalLengthLimit = sepal_length_limit)

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| extend ['SepalLengthLimit'] = 2.5"))
})

test_that("select and filter can be combined",
{
    q <- tbl_iris %>%
        dplyr::filter(Species == "setosa") %>%
        dplyr::select(Species, SepalLength)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| where ['Species'] == 'setosa'\n| project ['Species'], ['SepalLength']"))
})

test_that("select errors on column after selected away",
{
    q <- tbl_iris %>%
        dplyr::select(Species) %>%
        dplyr::select(SepalLength)

    expect_error(show_query(q), "object 'SepalLength' not found")
})

test_that("mutate translates to extend",
{
    q <- tbl_iris %>%
        dplyr::mutate(Species2 = Species)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| extend ['Species2'] = ['Species']"))
})

test_that("multiple arguments to mutate() become multiple extend clauses",
{
    q <- tbl_iris %>%
        dplyr::mutate(Species2 = Species, Species3 = Species2, Foo = 1 + 2)

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| extend ['Species2'] = ['Species']\n| extend ['Species3'] = ['Species2']\n| extend ['Foo'] = 1 + 2"))
})

test_that("sum() translated correctly",
{
    expect_equal(as.character(translate_kql(MeanSepalLength = mean(SepalLength, na.rm = TRUE))),
                 "avg(['SepalLength'])"
                 )
})

test_that("arrange() generates order by ",
{
    q <- tbl_iris %>%
        dplyr::arrange(Species, desc(SepalLength))

    q_str <- q %>%
        show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| order by ['Species'] asc, ['SepalLength'] desc"))
})

test_that("group_by() followed by summarize() generates summarize clause",
{

    q <- tbl_iris %>%
        dplyr::group_by(Species) %>%
        dplyr::summarize(MaxSepalLength = max(SepalLength, na.rm = TRUE))

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize ['MaxSepalLength'] = max(['SepalLength']) by ['Species']"))
})

test_that("group_by() followed by summarize() with multiple summarizations generates one summarize clause",
{

    q <- tbl_iris %>%
        dplyr::group_by(Species) %>%
        dplyr::summarize(MaxSepalLength = max(SepalLength, na.rm = TRUE),
                         MaxSepalWidth = max(SepalWidth, na.rm = TRUE))

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize ['MaxSepalLength'] = max(['SepalLength']), ['MaxSepalWidth'] = max(['SepalWidth']) by ['Species']"))
})

test_that("group_by() followed by summarize() with multiple summarizations generates one summarize clause in presence of hints",
{

    q <- tbl_iris %>%
        dplyr::group_by(Species) %>%
        dplyr::summarize(MaxSepalLength = max(SepalLength, na.rm = TRUE),
                         MaxSepalWidth = max(SepalWidth, na.rm = TRUE), .strategy="shuffle")

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize hint.strategy = shuffle ['MaxSepalLength'] = max(['SepalLength']), ['MaxSepalWidth'] = max(['SepalWidth']) by ['Species']"))
})

test_that("group_by() followed by ungroup() followed by summarize() generates summarize clause",
{

    q <- tbl_iris %>%
        dplyr::group_by(Species) %>%
        dplyr::summarize(MaxSepalLength = max(SepalLength, na.rm = TRUE)) %>%
        dplyr::ungroup() %>%
        dplyr::summarize(MeanOfMaxSepalLength = mean(MaxSepalLength, na.rm = TRUE))

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize ['MaxSepalLength'] = max(['SepalLength']) by ['Species']\n| summarize ['MeanOfMaxSepalLength'] = avg(['MaxSepalLength'])"))
})

test_that("group_by() followed by mutate() partitions the mutation by the grouping variables",
{

    q <- tbl_iris %>%
        dplyr::group_by(Species) %>%
        dplyr::mutate(SpeciesMaxSepalLength = max(SepalLength, na.rm = TRUE))

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| as tmp | join kind=leftouter (tmp | summarize ['SpeciesMaxSepalLength'] = max(['SepalLength']) by ['Species']) on ['Species']\n| project ['SepalLength'], ['SepalWidth'], ['PetalLength'], ['PetalWidth'], ['Species'], ['SpeciesMaxSepalLength']"))
})

test_that("mutate() with an agg function and no group_by() groups by all other columns",
{
    q <- tbl_iris %>%
        dplyr::mutate(MaxSepalLength = max(SepalLength, na.rm = TRUE))

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize ['MaxSepalLength'] = max(['SepalLength']) by ['SepalLength'], ['SepalWidth'], ['PetalLength'], ['PetalWidth'], ['Species']"))
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
        dplyr::rename(Species2 = Species, SepalLength2 = SepalLength)

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| project-rename ['Species2'] = ['Species'], ['SepalLength2'] = ['SepalLength']"))
})

test_that("rename() errors when given a nonexistent column",
{
    q <- tbl_iris %>%
        dplyr::rename(Species2 = Species1)

    expect_error(show_query(q), "object 'Species1' not found")
})

test_that("head(10) translates to take 10",
{
    q <- tbl_iris %>%
        head(10)

    q_str <- q %>% show_query()

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| take 10"))
})

test_that("head() translates to take 6 (the default)",
{
    q <- tbl_iris %>%
        head()

    q_str <- q %>% show_query

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| take 6"))
})

left <- tbl_iris

right <- iris %>%
    dplyr::group_by(Species) %>%
    dplyr::summarize(MaxSepalLength = max(Sepal.Length, na.rm = TRUE))

right2 <- iris %>%
    dplyr::rename(SepalWidth = Sepal.Width) %>%
    dplyr::group_by(Species, SepalWidth) %>%
    dplyr::summarize(MaxSepalLength = max(Sepal.Length, na.rm = TRUE))

right3 <- right2 %>% dplyr::rename(Species2 = Species, SepalWidth2 = SepalWidth)

right <- tbl_kusto_abstract(right, "iris2")

right2 <- tbl_kusto_abstract(right2, "iris2")

right3 <- tbl_kusto_abstract(right3, "iris3")

test_that("inner_join() on a single column translates correctly",
{
    q <- left %>%
        dplyr::inner_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = inner (cluster('local_df').database('local_df').['iris2']) on ['Species']"))
})

test_that("inner_join() on two columns translates correctly",
{
    q <- left %>%
        dplyr::inner_join(right2, by = c("Species", "SepalWidth"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = inner (cluster('local_df').database('local_df').['iris2']) on ['Species'], ['SepalWidth']"))
})

test_that("inner_join() on one differently named column translates correctly",
{
    q <- left %>%
        dplyr::inner_join(right3, by = c("Species" = "Species2"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = inner (cluster('local_df').database('local_df').['iris3']) on $left.['Species'] == $right.['Species2']"))
})

test_that("inner_join() on two differently named columns translates correctly",
{

    q <- left %>%
        dplyr::inner_join(right3, by = c("Species" = "Species2", "SepalWidth" = "SepalWidth2"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = inner (cluster('local_df').database('local_df').['iris3']) on $left.['Species'] == $right.['Species2'], $left.['SepalWidth'] == $right.['SepalWidth2']"))
})

test_that("left_join() on a single column translates correctly",
{

    q <- left %>%
        dplyr::left_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = leftouter (cluster('local_df').database('local_df').['iris2']) on ['Species']"))
})

test_that("right_join() on a single column translates correctly",
{

    q <- left %>%
        dplyr::right_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = rightouter (cluster('local_df').database('local_df').['iris2']) on ['Species']"))
})

test_that("full_join() on a single column translates correctly",
{

    q <- left %>%
        dplyr::full_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = fullouter (cluster('local_df').database('local_df').['iris2']) on ['Species']"))
})

test_that("semi_join() on a single column translates correctly",
{

    q <- left %>%
        dplyr::semi_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = leftsemi (cluster('local_df').database('local_df').['iris2']) on ['Species']"))
})

test_that("anti_join() on a single column translates correctly",
{

    q <- left %>%
        dplyr::anti_join(right, by = c("Species"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = leftanti (cluster('local_df').database('local_df').['iris2']) on ['Species']"))
})

test_that("union_all translates correctly",
{
    tbl_iris_2 <- tbl_kusto_abstract(iris, "iris")

    q <- tbl_iris %>%
        dplyr::union_all(tbl_iris_2)

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| union kind = outer (cluster('local_df').database('local_df').['iris'])"))

})

test_that("as.Date() produces a Kusto datetime",
{
    dates <- c("2019-01-01", "2019-01-02", "2019-01-03")
    dates <- as.Date(dates)
    words <- c("Tuesday", "Wednesday", "Thursday")
    df <- data.frame(dates, words)

    tbl_dates <- tbl_kusto_abstract(df, "df")

    q <- tbl_dates %>%
        filter(dates == as.Date("2019-01-01"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['df']\n| where ['dates'] == todatetime('2019-01-01')"))
})

test_that("as.POSIXct() produces a Kusto datetime",
{
    dates <- c("2019-01-01T23:59:59", "2019-01-02T23:59:58", "2019-01-03T00:00:00")
    dates <- as.POSIXct(strptime(dates, "%Y-%m-%dT%H:%M:%S", tz="UTC"))
    words <- c("Tuesday", "Wednesday", "Thursday")
    df <- data.frame(dates, words)

    tbl_dates <- tbl_kusto_abstract(df, "df")

    q <- tbl_dates %>%
        filter(dates == as.POSIXct(strptime("2019-01-01T23:59:59", "%Y-%m-%dT%H:%M:%S", tz="UTC")))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['df']\n| where ['dates'] == todatetime(todatetime('2019-01-01T23:59:59'))"))
})

test_that("as.POSIXlt() produces a Kusto datetime",
{
    dates <- c("2019-01-01", "2019-01-02", "2019-01-03")
    dates <- as.POSIXlt(dates)
    words <- c("Tuesday", "Wednesday", "Thursday")
    df <- data.frame(dates, words)

    tbl_dates <- tbl_kusto_abstract(df, "df")

    q <- tbl_dates %>%
        filter(dates == as.POSIXlt("2019-01-01"))

    q_str <- show_query(q)

    expect_equal(q_str, kql("cluster('local_df').database('local_df').['df']\n| where ['dates'] == todatetime('2019-01-01')"))
})

test_that("join hinting translates correctly",
{
    q <- left %>%
        dplyr::inner_join(right, by = c("Species"), .strategy="broadcast")
    q_str <- show_query(q)
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = inner hint.strategy = broadcast (cluster('local_df').database('local_df').['iris2']) on ['Species']"))

    q <- left %>%
        dplyr::inner_join(right2, by = c("Species", "SepalWidth"), .strategy="broadcast")
    q_str <- show_query(q)
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = inner hint.strategy = broadcast (cluster('local_df').database('local_df').['iris2']) on ['Species'], ['SepalWidth']"))

    q <- left %>%
        dplyr::inner_join(right2, by = c("Species", "SepalWidth"), .strategy="shuffle",
                          .shufflekeys=c("Species", "SepalWidth"))
    q_str <- show_query(q)
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = inner hint.strategy = shuffle hint.shufflekey = ['Species'] hint.shufflekey = ['SepalWidth'] (cluster('local_df').database('local_df').['iris2']) on ['Species'], ['SepalWidth']"))

    # only numeric input to .num_partitions allowed
    q <- left %>%
        dplyr::inner_join(right2, by = c("Species", "SepalWidth"), .strategy="shuffle",
                          .shufflekeys=c("Species", "SepalWidth"),
                          .num_partitions="foo")
    expect_error(show_query(q))

    q <- left %>%
        dplyr::inner_join(right2, by = c("Species", "SepalWidth"), .strategy="shuffle",
                          .shufflekeys=c("Species", "SepalWidth"),
                          .num_partitions=2)
    q_str <- show_query(q)
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| join kind = inner hint.strategy = shuffle hint.shufflekey = ['Species'] hint.shufflekey = ['SepalWidth'] hint.num_partitions = 2 (cluster('local_df').database('local_df').['iris2']) on ['Species'], ['SepalWidth']"))
})

test_that("summarize hinting translates correctly",
{
    q <- tbl_iris %>%
        dplyr::group_by(Species) %>%
        dplyr::summarize(MaxSepalLength = max(SepalLength, na.rm = TRUE), .strategy="shuffle")
    q_str <- q %>% show_query()
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize hint.strategy = shuffle ['MaxSepalLength'] = max(['SepalLength']) by ['Species']"))

    q <- tbl_iris %>%
        dplyr::group_by(Species) %>%
        dplyr::summarize(MaxSepalLength = max(SepalLength, na.rm = TRUE), .shufflekeys=c("SepalLength", "SepalWidth"))
    q_str <- q %>% show_query()
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize hint.shufflekey = ['SepalLength'] hint.shufflekey = ['SepalWidth'] ['MaxSepalLength'] = max(['SepalLength']) by ['Species']"))

    # only numeric input to .num_partitions allowed
    q <- tbl_iris %>%
        dplyr::group_by(Species) %>%
        dplyr::summarize(MaxSepalLength = max(SepalLength, na.rm = TRUE),
            .shufflekeys=c("SepalLength", "SepalWidth"), .num_partitions="foo")
    expect_error(show_query(q))

    q <- tbl_iris %>%
        dplyr::group_by(Species) %>%
        dplyr::summarize(MaxSepalLength = max(SepalLength, na.rm = TRUE),
            .shufflekeys=c("SepalLength", "SepalWidth"), .num_partitions=2)
    q_str <- q %>% show_query()
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize hint.shufflekey = ['SepalLength'] hint.shufflekey = ['SepalWidth'] hint.num_partitions = 2 ['MaxSepalLength'] = max(['SepalLength']) by ['Species']"))
})

test_that("unnest translates to mv-expand",
{

    list_df <- tibble::tibble(
        x = 1:2,
        y = list(a = 1, b = 3:4)
        )

    list_tbl <- tbl_kusto_abstract(list_df, table_name = "list_tbl")

    q <- list_tbl %>%
        tidyr::unnest(y)

    q_str <- show_query(q)
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['list_tbl']\n| mv-expand ['y']"))

})

test_that("unnest can handle multiple columns",
{

    list_df <- tibble::tibble(
        x = 1:2,
        y = list(a = 1, b = 3:4),
        z = list(c = 2, b = 5:6)
        )

    list_tbl <- tbl_kusto_abstract(list_df, table_name = "list_tbl")

    if (is_new_tidyr)
    {
        q <- list_tbl %>%
            tidyr::unnest(c(y, z))
    }
    else
    {
        q <- list_tbl %>%
            tidyr::unnest(y, z)
    }

    q_str <- show_query(q)
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['list_tbl']\n| mv-expand ['y'], ['z']"))

})

if (!is_new_tidyr)
{
    test_that("unnest .id translates to with_itemindex",
    {

        list_df <- tibble::tibble(
                               x = 1:2,
                               y = list(a = 1, b = 3:4)
                           )

        list_tbl <- tbl_kusto_abstract(list_df, table_name = "list_tbl")

        q <- list_tbl %>%
            tidyr::unnest(y, .id = "name")

        q_str <- show_query(q)

        expect_equal(q_str, kql("cluster('local_df').database('local_df').['list_tbl']\n| mv-expand with_itemindex=['name'] ['y']"))
    })
}

test_that("nest translates to summarize makelist()",
{
    if (is_new_tidyr)
    {
        q <- tbl_iris %>%
            tidyr::nest(data = c(SepalLength, SepalWidth, PetalLength, PetalWidth))
    }
    else
    {
        q <- tbl_iris %>%
            tidyr::nest(SepalLength, SepalWidth, PetalLength, PetalWidth)
    }


    q_str <- show_query(q)
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize ['SepalLength'] = make_list(['SepalLength']), ['SepalWidth'] = make_list(['SepalWidth']), ['PetalLength'] = make_list(['PetalLength']), ['PetalWidth'] = make_list(['PetalWidth']) by ['Species']"))

})

test_that("nest respects preceding group_by",
{
    q <- tbl_iris %>%
        group_by(Species) %>%
        tidyr::nest()

    q_str <- show_query(q)
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize ['SepalLength'] = make_list(['SepalLength']), ['SepalWidth'] = make_list(['SepalWidth']), ['PetalLength'] = make_list(['PetalLength']), ['PetalWidth'] = make_list(['PetalWidth']) by ['Species']"))
})

test_that("nest nests all non-provided columns",
{
    if (is_new_tidyr)
    {
        q <- tbl_iris %>%
        tidyr::nest(data = c(-Species))
    }
    else
    {
        q <- tbl_iris %>%
        tidyr::nest(-Species)
    }

    q_str <- show_query(q)
    expect_equal(q_str, kql("cluster('local_df').database('local_df').['iris']\n| summarize ['SepalLength'] = make_list(['SepalLength']), ['SepalWidth'] = make_list(['SepalWidth']), ['PetalLength'] = make_list(['PetalLength']), ['PetalWidth'] = make_list(['PetalWidth']) by ['Species']"))
})
