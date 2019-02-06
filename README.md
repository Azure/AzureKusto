# AzureKusto

R interface to Kusto, also known as [Azure Data Explorer](https://azure.microsoft.com/en-us/services/data-explorer/), a fast and highly scalable data exploration service.

## Installation

You can install the development version from GitHub, via `devtools::install_github("cloudyr/AzureKusto")`.

## Example Usage

### Kusto Endpoint Interface

Connect to a Kusto cluster by instantiating a `kusto_database_endpoint` object with the cluster URI, database name, and an `AzureRMR::AzureToken` object, which you can obtain via the `get_kusto_token` helper function.

```r

library(AzureKusto)

Samples <- kusto_database_endpoint(server="https://help.kusto.windows.net",
    database="Samples",
    .query_token=get_kusto_token(clustername="help", tenant="microsoft"))

# To sign in, use a web browser to open the page https://microsoft.com/devicelogin and enter the code FPD8GZPY9 to authenticate.
# Waiting for device code in browser...
# Press Esc/Ctrl + C to abort
# Authentication complete.
```

Now you can issue queries to the Kusto database with `run_query` and get the results back as a data.frame.

```r

res <- run_query(Samples, "StormEvents | summarize EventCount = count() by State | order by State asc")

head(res)

##            State EventCount
## 1        ALABAMA       1315
## 2         ALASKA        257
## 3 AMERICAN SAMOA         16
## 4        ARIZONA        340
## 5       ARKANSAS       1028
## 6 ATLANTIC NORTH        188

```

`run_query()` also supports query parameters. Simply pass your parameters as additional keyword arguments and they will be escaped and interpolated into the query string.

```r

res <- run_query(Samples, "MyFunction(lim)", lim=10L)

```

Command statements work much the same way, except that they do not accept parameters.

```r

res <- run_query(Samples, ".show tables | count")

```

### dplyr Interface

The package also implements a [dplyr](https://github.com/tidyverse/dplyr)-style interface for building a query upon a `tbl_kusto` object and then running it on the remote Kusto database and returning the result as a regular tibble object with `collect()`.

```r

library(dplyr)

StormEvents <- tbl_kusto(Samples, "StormEvents")

q <- StormEvents %>%
    group_by(State) %>%
    summarize(EventCount=n()) %>%
    arrange(State)

show_query(q)

# <KQL> database('Samples').StormEvents
# | summarize EventCount = count() by State
# | order by State asc

collect(q)

## # A tibble: 67 x 2
##    State          EventCount
##    <chr>               <dbl>
##  1 ALABAMA              1315
##  2 ALASKA                257
##  3 AMERICAN SAMOA         16
##  4 ARIZONA               340
##  5 ARKANSAS             1028
##  6 ATLANTIC NORTH        188
##  7 ATLANTIC SOUTH        193
##  8 CALIFORNIA            898
##  9 COLORADO             1654
## 10 CONNECTICUT           148
## # ... with 57 more rows

```

`tbl_kusto` also accepts query parameters, in case the Kusto source table is a parameterized function:

```r

MyFunctionDate <- tbl_kusto(Samples, "MyFunctionDate(dt)", dt=as.Date("2019-01-01"))

MyFunctionDate %>%
    select(StartTime, EndTime, EpisodeId, EventId, State) %>%
    head() %>%
    collect()

## # A tibble: 6 x 5
##   StartTime           EndTime             EpisodeId EventId State         
##   <dttm>              <dttm>                  <int>   <int> <chr>         
## 1 2007-09-29 08:11:00 2007-09-29 08:11:00     11091   61032 ATLANTIC SOUTH
## 2 2007-09-18 20:00:00 2007-09-19 18:00:00     11074   60904 FLORIDA       
## 3 2007-09-20 21:57:00 2007-09-20 22:05:00     11078   60913 FLORIDA       
## 4 2007-12-30 16:00:00 2007-12-30 16:05:00     11749   64588 GEORGIA       
## 5 2007-12-20 07:50:00 2007-12-20 07:53:00     12554   68796 MISSISSIPPI   
## 6 2007-12-20 10:32:00 2007-12-20 10:36:00     12554   68814 MISSISSIPPI   

```

---
[![cloudyr project logo](https://i.imgur.com/JHS98Y7.png)](https://github.com/cloudyr)
