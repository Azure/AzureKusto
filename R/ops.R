#' @export
op_base <- function(x, vars, class = character())
{
    stopifnot(is.character(vars))

    structure(
        list(
            x = x,
            vars = vars
        ),
        class = c(paste0("op_base_", class), "op_base", "op")
    )
}

op_base_local <- function(df)
{
    op_base(df, names(df), class = "local")
}

op_base_remote <- function(x, vars)
{
    op_base(x, vars, class = "remote")
}

#' @export
op_single <- function(name, x, dots = list(), args = list())
{
    structure(
        list(
            name = name,
            x = x,
            dots = dots,
            args = args
        ),
        class = c(paste0("op_", name), "op_single", "op")
    )
}

#' @export
add_op_single <- function(name, .data, dots = list(), args = list())
{
    .data$ops <- op_single(name, x = .data$ops, dots = dots, args = args)
    .data
}

#' @export
op_double <- function(name, x, y, args = list())
{
    structure(
        list(
            name = name,
            x = x,
            y = y,
            args = args
        ),
        class = c(paste0("op_", name), "op_double", "op")
    )
}

#' @export
add_op_join <- function(type, x, y, by = NULL, suffix = NULL, ...)
{
    by <- common_by(by, x, y)
    vars <- join_vars(op_vars(x), op_vars(y), type = type, by = by, suffix = suffix)
    x$ops <- op_double("join", x, y,
                       args = list(
                           vars = vars,
                           type = type,
                           by = by,
                           suffix = suffix
                       ))
    x
}

add_op_set_op <- function(x, y, type, ...)
{
    x$ops <- op_double("set_op", x, y, args = list(type = type))
    x
}

join_vars <- function(x_names, y_names, type, by, suffix = c(".x", ".y"))
{
    # Remove join keys from y's names
    y_names <- setdiff(y_names, by$y)

    if(!is.character(suffix) || length(suffix) != 2)
        stop("`suffix` must be a character vector of length 2.", call. = FALSE)

    suffix <- list(x = suffix[1], y = suffix[2])
    x_new <- add_suffixes(x_names, y_names, suffix$x)
    y_new <- add_suffixes(y_names, x_names, suffix$y)

    # In left and inner joins, return key values only from x
    # In right joins, return key values only from y
     # In full joins, return key values by coalescing values from x and y
    x_x <- x_names
    x_y <- by$y[match(x_names, by$x)]
    x_y[type == "left_join" | type == "inner_join"] <- NA
    x_x[type == "right_join" & !is.na(x_y)] <- NA
    y_x <- rep_len(NA, length(y_names))
    y_y <- y_names

    # Return a list with 3 parallel vectors
    # At each position, values in the 3 vectors represent
    #  alias - name of column in join result
    #  x - name of column from left table or NA if only from right table
    #  y - name of column from right table or NA if only from left table
    list(alias = c(x_new, y_new), x = c(x_x, y_x), y = c(x_y, y_y))
}

add_suffixes <- function(x, y, suffix)
{
    if (identical(suffix, "")) return(x)

    out <- chr_along(x)
    for (i in seq_along(x))
    {
        nm <- x[[i]]
        while (nm %in% y || nm %in% out)
            nm <- paste0(nm, suffix)

        out[[i]] <- nm
    }
    out
}

#' @export
op_grps <- function(op) UseMethod("op_grps")

#' @export
op_grps.op_base <- function(op) character()

#' @export
op_grps.op_group_by <- function(op)
{
    if (isTRUE(op$args$add))
        union(op_grps(op$x), names(op$dots))
    else
        names(op$dots)
}

#' @export
op_grps.op_ungroup <- function(op)
{
    character()
}

#' @export
op_grps.op_summarise <- function(op)
{
    grps <- op_grps(op$x)
}

#' @export
op_grps.op_rename <- function(op)
{
    names(tidyselect::vars_rename(op_grps(op$x), !!! op$dots, .strict = FALSE))
}

#' @export
op_grps.op_single <- function(op)
{
    op_grps(op$x)
}
#' @export
op_grps.op_double <- function(op)
{
    op_grps(op$x)
}

#' @export
op_grps.tbl_kusto_abstract <- function(op)
{
    op_grps(op$ops)
}

#' @export
op_grps.tbl_df <- function(op)
{
    character()
}

#' @export
op_vars <- function(op) UseMethod("op_vars")

#' @export
op_vars.op_base <- function(op)
{
    op$vars
}

#' @export
op_vars.op_select <- function(op)
{
    names(tidyselect::vars_select(op_vars(op$x), !!! op$dots, .include = op_grps(op$x)))
}

#' @export
op_vars.op_rename <- function(op)
{
    names(rename_vars(op_vars(op$x), !!! op$dots))
}

#' @export
op_vars.op_summarise <- function(op)
{
    c(op_grps(op$x), names(op$dots))
}

#' @export
op_vars.op_distinct <- function(op)
{
    if (is_empty(op$dots))
        op_vars(op$x)
    else
        unique(c(op_vars(op$x), names(op$dots)))
}

#' @export
op_vars.op_mutate <- function(op)
{
    unique(c(op_vars(op$x), names(op$dots)))
}

#' @export
op_vars.op_single <- function(op)
{
    op_vars(op$x)
}

#' @export
op_vars.op_join <- function(op)
{
    op$args$vars$alias
}

#' @export
op_vars.op_join <- function(op)
{
    op$args$vars$alias
}

#' @export
op_vars.op_semi_join <- function(op)
{
    op_vars(op$x)
}

#' @export
op_vars.op_set_op <- function(op)
{
    union(op_vars(op$x), op_vars(op$y))
}

#' @export
op_vars.tbl_kusto_abstract <- function(op)
{
    op_vars(op$ops)
}

#' @export
op_vars.tbl_df <- function(op)
{
    names(op)
}
