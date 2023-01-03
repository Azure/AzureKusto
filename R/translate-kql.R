#' Translate R expressions into Kusto Query Language equivalents.
#' @param ... Expressions to translate.
#' @export
translate_kql <- function(...)
{
    dots <- quos(...)

    if (is_empty(dots)) return(kql())

    stopifnot(is.list(dots))

    if (!any(have_name(dots))) names(dots) <- NULL

    variant <- kql_translate_env()
    pieces <- lapply(dots, function(x)
    {
        if (is_atomic(get_expr(x)))
            escape(get_expr(x))
        else {
            mask <- kql_mask(x, variant)
            escape(eval_tidy(x, mask))
        }
    })

    kql(unlist(pieces))

}

kql_mask <- function(expr, variant)
{
    # Default for unknown functions
    unknown <- setdiff(all_calls(expr), names(variant))
    top_env <- ceply(unknown, default_op, parent = empty_env())

    # Known R -> KQL functions
    special_calls <- copy_env(variant$scalar, parent = top_env)
    special_calls2 <- copy_env(variant$aggregate, parent = special_calls)

    # Existing symbols in expression
    names <- all_names(expr)
    idents <- lapply(names, ident)
    name_env <- ceply(idents, escape, parent = special_calls2)

    # Known kql expressions
    symbol_env <- env_clone(base_symbols, parent = name_env)

    new_data_mask(symbol_env, top_env)
}

# character vector -> environment
ceply <- function(x, f, ..., parent = parent.frame())
{
    if (is_empty(x)) return(new.env(parent = parent))

    l <- lapply(x, f, ...)
    names(l) <- x
    list2env(l, parent = parent)
}

#' Build a kql_variant class out of the environments holding scalar and aggregation
#' function definitions
#' @export
kql_translate_env <- function()
{
    kql_variant(
        base_scalar,
        base_agg
    )
}

kql_variant <- function(scalar = kql_translator(), aggregate = kql_translator())
{
    stopifnot(is.environment(scalar))
    stopifnot(is.environment(aggregate))

    structure(
        list(scalar = scalar, aggregate = aggregate),
        class = "kql_variant"
    )
}

#' Builds an environment from a list of R -> Kusto query language translation pairs.
#' @param ... Pairs of R call = Kusto call translations as individual arguments
#' @param .funs Parse of R call = Kusto call translations in list format
#' @param .parent A parent environment to attach this env onto
#' @export
kql_translator <- function(..., .funs = list(),
                           .parent = new.env(parent = emptyenv()))
{
    funs <- c(list(...), .funs)

    if (is_empty(funs)) return(.parent)

    list2env(funs, copy_env(.parent))
}

copy_env <- function(from, to = NULL, parent = parent.env(from))
{
    list2env(as.list(from), envir = to, parent = parent)
}

#' Return a function representing a scalar KQL infix operator
#' @param f Name of a Kusto infix operator / function
#' @export
kql_infix <- function(f)
{
    stopifnot(is.character(f))

    function(x, y) {
        build_kql(x, " ", kql(f), " ", y)
    }
}

#' Return a function representing a scalar KQL prefix function
#' @param f Name of a Kusto infix function
#' @param n Number of arguments accepted by the Kusto prefix function
#' @export
kql_prefix <- function(f, n = NULL)
{
    stopifnot(is.character(f))

    function(...)
    {
        args <- list(...)
        if (!is.null(n) && length(args) != n) {
            stop(
                "Invalid number of args to ", f, ". Expecting ", n,
                call. = FALSE
            )
        }

        if (any(names2(args) != ""))
            warning("Named arguments ignored for ", f, call. = FALSE)

        build_kql(kql(f), args)
    }
}

#' Return a function representing a KQL aggregation function
#' @param f Name of the Kusto aggregation function
#' @export
kql_aggregate <- function(f)
{
    stopifnot(is.character(f))

    function(x, na.rm = FALSE) {
        check_na_rm(f, na.rm)
        build_kql(kql(f), list(x))
    }
}

#' Return a function representing a KQL window function
#' @param f Name of the Kusto aggregation function
#' @export
kql_window <- function(f)
{
    stopifnot(is.character(f))

    function(x, na.rm = FALSE) {
        check_na_rm(f, na.rm)
        build_kql(kql(f), list(x))
    }
}

check_na_rm <- function(f, na.rm)
{
    if (identical(na.rm, TRUE)) return()

    warning(
        "Missing values are always removed in KQL.\n",
        "Use `", f, "(x, na.rm = TRUE)` to silence this warning",
        call. = FALSE
    )
}

all_names <- function(x)
{
    if (is.name(x)) return(as.character(x))
    if (!is.call(x)) return(NULL)

    unique(unlist(lapply(x[-1], all_names), use.names = FALSE))
}

all_calls <- function(x) {
  if (!is.call(x)) return(NULL)

  fname <- as.character(x[[1]])
  unique(c(fname, unlist(lapply(x[-1], all_calls), use.names = FALSE)))
}

is_infix_base <- function(x)
{
    x %in% c("::", "$", "@", "^", "*", "/", "+", "-", ">", ">=", "<", "<=",
             "==", "!=", "!", "&", "&&", "|", "||", "~", "<-", "<<-")
}
is_infix_user <- function(x)
{
    grepl("^%.*%$", x)
}

default_op <- function(x)
{
    stopifnot(is.character(x))

    if (is_infix_base(x))
    {
        kql_infix(x)
    }
    else if (is_infix_user(x))
    {
        x <- substr(x, 2, nchar(x) - 1)
        kql_infix(x)
    }
    else
    {
        kql_prefix(x)
    }


}

#' Scalar operator translations (infix and prefix)
#' @export
base_scalar <- kql_translator(
    `+`    = kql_infix("+"),
    `*`    = kql_infix("*"),
    `/`    = kql_infix("/"),
    `%%`   = kql_infix("%"),
    `^`    = kql_prefix("power", 2),
    `-`    = function(x, y = NULL)
    {
        if (is.null(y))
            if (is.numeric(x))
                -x
            else
                build_kql(kql("-"), x)
        else
            build_kql(x, kql(" - "), y)
    },
    `!=`    = kql_infix("!="),
    `==`    = kql_infix("=="),
    `<`     = kql_infix("<"),
    `<=`    = kql_infix("<="),
    `>`     = kql_infix(">"),
    `>=`    = kql_infix(">="),

    `%in%` = function(x, table) {
        if (is.kql(table) || length(table) > 1) {
            build_kql(x, " IN ", table)
        } else {
            build_kql(x, " IN (", table, ")")
        }
    },

    `!`     = kql_prefix("not"),
    `&`     = kql_infix("and"),
    `&&`    = kql_infix("and"),
    `|`     = kql_infix("or"),
    `||`    = kql_infix("or"),
    xor     = function(x, y) {
        kql(sprintf("(%1$s or %2$s) and not (%1$s and %2$s)", escape(x), escape(y)))
    },

    abs     = kql_prefix("abs", 1),
    acos    = kql_prefix("acos", 1),
    asin    = kql_prefix("asin", 1),
    atan    = kql_prefix("atan", 1),
    atan2   = kql_prefix("atan2", 2),
    ceil    = kql_prefix("ceiling", 1),
    ceiling = kql_prefix("ceiling", 1),
    cos     = kql_prefix("cos", 1),
    cot     = kql_prefix("cot", 1),
    exp     = kql_prefix("exp", 1),
    floor   = kql_prefix("floor", 1),
    log     = function(x, base = exp(1))
    {
        if (isTRUE(all.equal(base, exp(1))))
        {
            prefix("log")
        } else if (is.TRUE(all.equal(base, 2)))
        {
            prefix("log2")
        } else if (is.TRUE(all.equal(base, 10)))
        {
            prefix("log10")
        } else
        {
            stop("KQL only supports logarithms with base e, 2, or 10.")
        }
    },
    log10   = kql_prefix("log10", 1),
    round   = kql_prefix("round", 2),
    sign    = kql_prefix("sign", 1),
    sin     = kql_prefix("sin", 1),
    sinh    = kql_prefix("sinh", 1),
    sqrt    = kql_prefix("sqrt", 1),
    tan     = kql_prefix("tan", 1),
    tanh     = kql_prefix("tanh", 1),

    tolower = kql_prefix("tolower", 1),
    toupper = kql_prefix("toupper", 1),
    #trimws = kql_prefix("trim", 1),
    nchar   = kql_prefix("strlen", 1),
    substr = function(x, start, stop) {
        start <- as.integer(start)
        length <- pmax(as.integer(stop) - start + 1L, 0L)

        build_kql(kql("substring"), list(x, start, length))
    },

    `if` = kql_prefix("iif", 3),
    if_else = kql_prefix("iif", 3),
    ifelse = kql_prefix("iif", 3),
    case_when = kql_prefix("case"),
    kql = function(...) kql(...),
    `(` = function(x) {
        build_kql("(", x, ")")
    },
    `{` = function(x) {
        build_kql("(", x, ")")
    },
    `$` = kql_infix("."),
    asc = function(x) {
        build_kql(x, kql(" asc"))
    },
    desc = function(x) {
        build_kql(x, kql(" desc"))
    },

    is.null = kql_prefix("isnull"),
    is.na = kql_prefix("isnull"),
    coalesce = kql_prefix("coalesce"),
    as.numeric = kql_prefix("todouble", 1),
    as.double = kql_prefix("todouble", 1),
    as.integer = kql_prefix("toint", 1),
    as.character = kql_prefix("tostring", 1),
    as.Date = kql_prefix("todatetime", 1),
    as.POSIXct = kql_prefix("todatetime", 1),
    as.POSIXlt = kql_prefix("todatetime", 1),
    strptime = function(dt_str, format_str, tz="UTC") {
        if(tz != "UTC") {
            warning("Kusto only supports datetimes in UTC timezone. Non-UTC datetimes will be cast as UTC.")
        }
        kql_prefix("todatetime", 1)(dt_str)
    },

    c = function(...) c(...),
    `:` = function(from, to) from:to,

    between = function(x, left, right) {
        build_kql(x, " between (", left, " .. ", right, ")")
    },

    pmin = kql_prefix("min"),
    pmax = kql_prefix("max"),

    `%>%` = `%>%`,
    str_length      = kql_prefix("strlen"),
    str_to_upper    = kql_prefix("toupper"),
    str_to_lower    = kql_prefix("tolower"),
    str_replace_all = function(string, pattern, replacement){
        build_kql(
            "replace(", pattern, ", ", replacement, ", ", string, ")"
        )},
    str_detect      = function(string, pattern){
        build_kql(
            string, " matches regex ", pattern
        )},
    str_trim        = function(string, side = "both"){
        build_kql(
            kql(ifelse(side == "both" | side == "left", "trim_start(' '", "(")),
            kql(ifelse(side == "both" | side == "right", "trim_end(' '", "(")),
            string,
            "))"
        )}
)

#' Tag character strings as Kusto Query Language. Assumes the string is valid and properly escaped.
#' @param ... character strings to tag as KQL
#' @export
kql <- function(...)
{
    x <- c_character(...)
    structure(x, class = c("kql", "character"))
}

#' @export
print.kql <- function(x, ...)
{
    cat("<KQL> ", x, "\n", sep = "")
}


base_symbols <- kql_translator(
    pi = kql("pi()"),
    `NULL` = kql("nupll")
)

#' Aggregation function translations
#' @export
base_agg <- kql_translator(
    n          = function() kql("count()"),
    mean       = kql_aggregate("avg"),
    var        = kql_aggregate("variance"),
    sum        = kql_aggregate("sum"),
    min        = kql_aggregate("min"),
    max        = kql_aggregate("max"),
    n_distinct = kql_aggregate("dcount")
)

#' Window function translations
#' @export
base_window <- kql_translator(
    row_number = kql_window("row_number")
)

dots <- function(...)
{
    eval_bare(substitute(alist(...)))
}

is_agg <- function(f)
{
    ef <- enexpr(f)

    if (is.symbol(ef))
        sf <- as_string(ef)
    else if (typeof(ef) == "character")
        sf <- ef
    else
        return(FALSE)

    sf %in% ls(base_agg)
}
