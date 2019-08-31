#' Build the tbl object into a data structure representing a Kusto query
#' @param op A nested sequence of query operations, i.e. tbl_kusto$ops
#' @export
kql_build <- function(op)
{
    UseMethod("kql_build")
}

#' @export
kql_build.tbl_kusto_abstract <- function(op)
{
    q <- flatten_query(op$ops)
    built_q <- lapply(q, kql_build)
    kql_query(built_q, src=op$src)
}


#' @export
kql_build.op_base_local <- function(op, ...)
{
    ident("df")
}

#' @export
kql_build.op_base_remote <- function(op, ...)
{
    ident(op$src$x)
}

#' @export
kql_build.op_select <- function(op, ...)
{
    kql_clause_select(translate_kql(!!! op$dots))
}

#' @export
kql_build.op_filter <- function(op, ...)
{
    dots <- mapply(get_expr, op$dots)
    dot_names <- mapply(all_names, dots)

    # throw an exception if any filter expression references
    # a var that isn't a column in the table
    tidyselect::vars_select(op$vars, !!! dot_names)

    translated_dots <- lapply(dots, translate_kql)
    built_dots <- lapply(translated_dots, build_kql)
    clauses <- lapply(built_dots, kql_clause_filter)
    clauses
}

#' @export
kql_build.op_distinct <- function(op, ...)
{
    if (is_empty(op$dots))
        cols <- op$vars
    else
        cols <- tidyselect::vars_select(op$vars, !!! op$dots)

    kql_clause_distinct(ident(cols))
}

#' @export
kql_build.op_rename <- function(op, ...)
{
    assigned_exprs <- mapply(get_expr, op$dots)
    stmts <- lapply(assigned_exprs, translate_kql)
    pieces <- lapply(seq_along(assigned_exprs),
                     function(i) sprintf("%s = %s", escape(ident(names(assigned_exprs)[i])), stmts[i]))
    kql(paste0("project-rename ", paste0(pieces, collapse=", ")))
}

#' dplyr's mutate verb can include aggregations, but Kusto's extend verb cannot.
#' If the mutate contains no aggregations, then it can emit an extend clause.
#' If the mutate contains an aggregation and the tbl is ungrouped,
#' then it must emit a summarize clause grouped by all variables.
#' If the mutate contains an aggregation and the tbl is grouped,
#' then it must join to a subquery containing the summarize clause.
#' @param op A nested sequence of query operations, i.e. tbl_kusto$ops
#' @param ... Needed for agreement with generic. Not otherwise used.
#' @export
kql_build.op_mutate <- function(op, ...)
{
    assigned_exprs <- mapply(get_expr, op$dots)
    calls <- unlist(mapply(all_calls, assigned_exprs))
    calls_agg <- mapply(is_agg, calls)
    groups <- build_kql(escape(ident(op$groups), collapse = ", "))
    all_vars <- build_kql(escape(ident(op$vars), collapse = ", "))
    existing_vars <- build_kql(escape(ident(setdiff(op$vars, names(assigned_exprs))), collapse = ", "))

    if (any(calls_agg))
    {
        has_agg <- TRUE
        if (nchar(groups) == 0) {
            has_grouping <- FALSE
            verb <- "summarize "
            by <- build_kql(" by ", existing_vars)
        } else
        {
            has_grouping <- TRUE
            verb <- "as tmp | join kind=leftouter (tmp | summarize "
            by <- build_kql(" by ", groups)
            on <- build_kql(") on ", groups)
            project <- build_kql("\n| project ", all_vars)
            by <- paste0(by, on, project)
        }
    }
    else
    {
        has_agg <- FALSE
        verb <- "extend "
        by <- ""
    }

    stmts <- mapply(translate_kql, assigned_exprs)
    pieces <- lapply(seq_along(assigned_exprs),
                     function(i) sprintf("%s = %s", escape(ident(names(assigned_exprs)[i])), stmts[i]))
    kql(paste0(verb, pieces, by))
}

#' @export
kql_build.op_arrange <- function(op, ...)
{
    dots <- mapply(append_asc, op$dots)
    order_vars <- translate_kql(!!! dots)
    build_kql("order by ", build_kql(escape(order_vars, collapse = ", ")))
}

#' @export
kql_build.op_summarise <- function(op, ...)
{
    assigned_exprs <- mapply(get_expr, op$dots)
    stmts <- mapply(translate_kql, assigned_exprs)
    pieces <- lapply(seq_along(assigned_exprs),
                     function(i) sprintf("%s = %s", escape(ident(names(assigned_exprs)[i])), stmts[i]))
    groups <- build_kql(escape(ident(op_grps(op)), collapse = ", "))
    by <- ifelse(nchar(groups) > 0, paste0(" by ", groups), "")

    .strategy <- if(!is.null(op$args$.strategy))
        paste0(" hint.strategy = ", op$args$.strategy)
    else NULL

    .shufflekeys <- if(!is.null(op$args$.shufflekeys))
    {
        vars <- sapply(op$args$.shufflekeys, function(x) escape(ident(x)))
        paste0(" hint.shufflekey = ", vars, collapse="")
    }
    else NULL

    .num_partitions <- if(is.numeric(op$args$.num_partitions))
        paste0(" hint.num_partitions = ", op$args$.num_partitions)
    else if(!is.null(op$args$.num_partitions))
        stop(".num_partitions must be a number", .call=FALSE)
    else NULL

    # paste(c(*), collapse="") will not insert extra spaces when NULLs present
    smry_str <- paste(c("summarize", .strategy, .shufflekeys, .num_partitions, " "), collapse="")
    smry_clauses <- paste(pieces, collapse=", ")
    kql(ident_q(paste0(smry_str, smry_clauses, by)))
}

#' @export
kql_build.op_group_by <- function(op, ...)
{
    NULL
}

#' @export
kql_build.op_ungroup <- function(op, ...)
{
    NULL
}

#' @export
kql_build.op_unnest <- function(op, ...)
{
    if (!is.null(op$args$.id))
    {
        with_itemindex <- build_kql("with_itemindex=", escape(ident(op$args$.id)), " ")
    } else
    {
        with_itemindex <- kql("")
    }

    cols_to_unnest <- unname(tidyselect::vars_select(op_vars(op), !!! op$dots))

    if (is_empty(cols_to_unnest))
        cols_to_unnest <- setdiff(op_vars(op), op_grps(op))

    build_kql("mv-expand ", with_itemindex, build_kql(escape(ident(cols_to_unnest), collapse = ", ")))
}

#' @export
kql_build.op_head <- function(op, ...)
{
    n <- lapply(op$args$n, translate_kql)
    build_kql("take ", kql(escape(n, parens = FALSE)))
}

#' @export
kql_build.op_join <- function(op, ...)
{
    join_type <- op$args$type

    by <- op$args$by
    by_x <- escape(ident(by$x))
    if (identical(by$x, by$y))
        by_clause <- by_x
    else
    {
        by_y <- escape(ident(by$y))
        by_clause <- kql(ident(paste0(mapply(build_by_clause, by$x, by$y), collapse = ", ")))
    }

    y_render <- kql(kql_render(kql_build(op$y)))

    .strategy <- if(!is.null(op$args$.strategy))
        paste0(" hint.strategy = ", op$args$.strategy)
    else NULL

    .shufflekeys <- if(!is.null(op$args$.shufflekeys))
    {
        vars <- sapply(op$args$.shufflekeys, function(x) escape(ident(x)))
        paste0(" hint.shufflekey = ", vars, collapse="")
    }
    else NULL

    .num_partitions <- if(is.numeric(op$args$.num_partitions))
        paste0(" hint.num_partitions = ", op$args$.num_partitions)
    else if(!is.null(op$args$.num_partitions))
        stop(".num_partitions must be a number", .call=FALSE)
    else NULL

    .remote <- if(!is.null(op$args$.remote))
        paste0(" hint.remote = ", op$args$.remote, collapse="")
    else NULL

    kind <- switch(join_type,
        inner_join="inner",
        left_join="leftouter",
        right_join="rightouter",
        full_join="fullouter",
        semi_join="leftsemi",
        anti_join="leftanti",
        stop("unknown join type")
    )

    # paste(c(*), collapse="") will not insert extra spaces when NULLs present
    join_str <- ident_q(paste(c("join kind = ", kind, .strategy, .shufflekeys, .num_partitions, .remote, " "),
        collapse=""))
    build_kql(join_str, "(", y_render, ") on ", by_clause)
}

#' @export
kql_build.op_set_op <- function(op, ...)
{
    op_type <- op$args$type

    y_render <- kql(kql_render(kql_build(op$y)))

    switch(op_type,
        union_all=
            build_kql("union kind = outer (", y_render, ")"),
        build_kql("union kind = inner (", y_render, ")")
    )
}

append_asc <- function(dot)
{
    if (inherits(dot[[2]], "name"))
        dot[[2]] <- call2(expr(asc), dot[[2]])
    else if (inherits(dot[[2]], "call"))
        if (dot[[2]][[1]] != expr("desc"))
            dot[[2]] <- call2(expr(asc), dot[[2]])
        else
            dot
    else
        dot
}

#' Walks the tree of ops and builds a stack.
#' @param op the current operation
#' @param ops the stack of operations to append to, recursively
#' @export
flatten_query <- function(op, ops=list())
{
    if (inherits(op, "tbl_df") || inherits(op, "character"))
        return(ops)

    if (inherits(op, "tbl_kusto_abstract"))
        flat_op <- op$ops
    else
        flat_op <- op

    flat_op$vars <- op_vars(flat_op)
    flat_op$groups <- op_grps(flat_op)

    if (is_empty(ops))
        new_ops <- list(flat_op)
    else
        new_ops <- c(list(flat_op), ops)
    if (inherits(op, "op_base"))
        return(new_ops)
    else
        flatten_query(flat_op$x, new_ops)
}

kql_clause_select <- function(select)
{
    stopifnot(is.character(select))
    if (is_empty(select))
        abort("Query contains no columns")

    build_kql(
        "project ",
        escape(select, collapse = ", ")
    )
}

kql_clause_distinct <- function(distinct)
{
    stopifnot(is.character(distinct))

    build_kql(
        "distinct ",
        escape(distinct, collapse = ", ")
    )
}

kql_clause_filter <- function(where)
{
    if (!is_empty(where))
    {
        where_paren <- escape(where, parens = FALSE)
        build_kql("where ", kql_vector(where_paren, collapse = " and "))
    }
}

kql_query <- function(ops, src)
{
    structure(
        list(
            ops = ops,
            src = src
        ),
        class = "kql_query"
    )
}

build_by_clause <- function(x, y )
{
    sprintf("$left.%s == $right.%s", escape(ident(x)), escape(ident(y)))
}
