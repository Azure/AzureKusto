#' @export
tbl <- function(object, ...)
{
    UseMethod("tbl")
}


#' @export
tbl.ade_database <- function(object, table, ...)
{
    out <- list(tbl=table, db=object)
    class(out) <- "tbl"
    out
}

#' @export
select.tbl <- function(.data, ...)
{
    dots <- quos(...)
    add_op_single("select", .data, dots = dots)
    ## grps <- group_vars(.data)
    ## vars <- unique(c(grps, select_vars(names(.data), ...)))
    ## if(length(vars) == 0)
    ##     stop("No variables selected", call.=FALSE)

    ## arglst <- list(.data, varsToKeep=vars)
    ## arglst <- doExtraArgs(arglst, .data, enexpr(.rxArgs), .outFile)

    ## on.exit(deleteIfTbl(.data))
    ## # need to use rxImport on non-Xdf data sources because of bugs in rxDataStep (?)
    ## output <- callRx("rxDataStep", arglst)
    ## simpleRegroup(output, grps)
}


#' @export
add_op_single <- function(name, .data, dots = list(), args = list()) {
  .data$ops <- op_single(name, x = .data$ops, dots = dots, args = args)
  .data
}

#' @export
op_single <- function(name, x, dots = list(), args = list()) {
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
