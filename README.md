# AzureKusto

R interface to Kusto, also known as [Azure Data Explorer](https://azure.microsoft.com/en-us/services/data-explorer/), a fast and highly scalable data exploration service.

## Installation

You can install the development version from GitHub, via `devtools::install_github("cloudyr/AzureKusto")`.

## Example Usage

### Kusto Endpoint Interface

Connect to a Kusto cluster by instantiating a `kusto_query_endpoint` object with the cluster URI, database name, and an `AzureRMR::AzureToken` object, which you can obtain via the `get_kusto_token` helper function.

```r

library(AzureKusto)

Samples <- kusto_query_endpoint(server="https://help.kusto.windows.net",
    database="Samples",
    .azure_token=get_kusto_token(clustername="help", tenant="microsoft"))

# To sign in, use a web browser to open the page https://microsoft.com/devicelogin and enter the code FPD8GZPY9 to authenticate.
# Waiting for device code in browser...
# Press Esc/Ctrl + C to abort
# Authentication complete.
```

Now you can issue queries to the Kusto database with `run_query` and get the results back as a data.frame.

```r

res <- run_query(Samples, "StormEvents | summarize EventCount = count() by State | order by State asc")

as_tibble(res)

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

---
[![cloudyr project logo](https://i.imgur.com/JHS98Y7.png)](https://github.com/cloudyr)
