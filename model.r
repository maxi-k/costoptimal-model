source("./util.r")

## Use parallel evaluation if possible
if (util.is.shiny.deployed) {
  future::plan(future::sequential)
} else {
  future::plan(future::multicore, workers = round(future::availableCores() * 0.75))
}

## ---------------------------------------------------------------------------------------------- ##
                                        # Model Constants
## ---------------------------------------------------------------------------------------------- ##

## GB/s
model.factors.bandwidth <- list(
  RAM  = 50,
  NVMe = 2,
  SSD  = 0.5,
  HDD  = 0.25,
  EBS  = NA
)

## ---------------------------------------------------------------------------------------------- ##
                                        # Calculation Helpers
## ---------------------------------------------------------------------------------------------- ##

model.make.scaling.fn <- function(p) {
  function(n) {
    (1 - p$p) + (p$p / n)
  }
}

model.make.distr.fn <- function(shape) {
  function(size) {
    if (is.na(size) || length(size) == 0) {
      return(c())
    }
    if (size <= 1) {
      return(c(size))
    }
    distr <- dzipf(1:size, size, shape)
    normd <- distr / sum(distr) * size
    normd
  }
}

## bins : list(
##   [binname] : list(size : number, prio : number)
##        # where:
##                  # size:  Size of each bin
##                  # prio:  Priorities for each bins (high to low)
## )
## distr : numeric  # Distribution to pack bins
##
## returns : data.frame(
##    [binname] : number   # Amount that bin is filled
## )
model.distr.pack <- function(bins, distr) {
    n <- length(bins[[1]]$prio)
    if (n > 1) {
        res <- lapply(1:n, function(i) {
            model.distr.pack(
                purrr::map(bins, ~ list(size = .$size[i], prio = .$prio[i])),
                distr
            )
        })
        res <- as.data.frame(plyr::rbind.fill.matrix(res))
        res[setdiff(names(bins), names(res))] <- 0
        res[is.na(res)] <- 0
        return(res)
    }
    distr.len <- length(distr)
    prios <- bins[order(sapply(bins, function(x) -x$prio ))]
    sizes <- sapply(prios, `[`, "size")
    names.prio <- (names(sizes) <- names(prios))
    group.list <- slide(cumsum(sizes), .before = 1, function(sizes) {
        if (length(sizes) == 1)
            rep(names(sizes), min(sizes[[1]], distr.len))
        else if (sizes[[1]] > distr.len)
            c()
        else
            rep(names(sizes)[2], min(sizes[[2]], distr.len) - sizes[[1]])
    })
    groups <- unlist(group.list, use.names = FALSE)
    sums <- t(rowsum(distr, groups, reorder = FALSE))
    sums
}

## split the given distribution horizontally at the given limit
##
## returns:  list(distr_<lim, distr>lim)
##
model.distr.hsplit <- function(distr, lim) {
    dist.low <- pmin(distr, lim)
    dist.high <- pmax(distr - dist.low, 0)
    list(dist.low, dist.high)
}

model.distr.split.fn <- function(.split.first.read) {
    function(.dist) {
        if(.split.first.read) {
            splitdist <- model.distr.hsplit(.dist, 1)
        } else {
            splitdist <- list(rep(0, length(.dist)), .dist)
        }
        names(splitdist) <- c("initial", "working")
        splitdist
    }
}

## ---------------------------------------------------------------------------------------------- ##
                                        # Calculate Timings
## ---------------------------------------------------------------------------------------------- ##

model.calc.time.for.config <- function(.inst, .count, .query, .distr.cache, .distr.spool, .n.eff, .time.period) {
    .bins.cache <- list(
        data.mem = list(size = round(.inst$calc.mem.caching), prio = .inst$calc.mem.speed),
        data.sto = list(size = round(.inst$calc.sto.caching), prio = .inst$calc.sto.speed),
        data.s3  = list(size = rep(length(.distr.cache$working), times = nrow(.inst)),
                        prio = .inst$calc.net.speed)
    )

    .bins.spool <- list(
        data.mem = list(size = round(.inst$calc.mem.spooling), prio = .inst$calc.mem.speed),
        data.sto = list(size = round(.inst$calc.sto.spooling), prio = .inst$calc.sto.speed),
        data.s3  = list(size = rep(length(.distr.spool), times = nrow(.inst)),
                        prio = .inst$calc.net.speed)
    )

    .pack.cache <- model.distr.pack(.bins.cache, .distr.cache$working) %>% as.data.frame()
    .pack.spool <- model.distr.pack(.bins.spool, .distr.spool) %>% as.data.frame()
    .spool.sum  <- sum(.distr.spool)
    .inv.eff <- .count * .n.eff

    dplyr::transmute(.inst,
                     id.name          = id,
                     count            = .count,
                     id               = paste(id.name, count, sep="/"),
                     cost.usdph       = cost.usdph * .count,

                     read.cache.load  = sum(.distr.cache$initial),
                     read.cache.mem   = .pack.cache$data.mem %||% 0,
                     read.cache.sto   = .pack.cache$data.sto %||% 0,
                     read.cache.s3    = .pack.cache$data.s3 %||% 0,

                     read.spool.mem   = .pack.spool$data.mem %||% 0,
                     read.spool.sto   = .pack.spool$data.sto %||% 0,
                     read.spool.s3    = .pack.spool$data.s3 %||% 0,

                     rw.mem           = read.cache.mem + 2 * read.spool.mem,
                     rw.sto           = read.cache.sto + 2 * read.spool.sto,
                     rw.s3            = read.cache.s3  + 2 * read.spool.s3,
                     rw.xchg          = if_else(.count == 1, 0.0, 2 * .spool.sum),

                     stat.read.spool  = .spool.sum,
                     stat.read.work   = sum(.distr.cache$working),

                     time.cpu         = (.query$time.cpu * 3600 / calc.cpu.real) * .n.eff,
                     time.mem         = (rw.mem          / calc.mem.speed) * .inv.eff,
                     time.sto         = (rw.sto          / calc.sto.speed) * .inv.eff,
                     time.s3          = (rw.s3           / calc.s3.speed ) * .inv.eff,
                     time.xchg        = (rw.xchg / 2     / calc.net.speed) * .inv.eff,
                     time.load        = (read.cache.load / calc.s3.speed ) * .inv.eff,

                     stat.time.sum    = time.s3 + time.sto + time.cpu + time.xchg + time.load + time.mem,
                     stat.time.max    = pmax(time.s3, time.sto, time.cpu, time.xchg, time.load, time.mem),
                     stat.time.period = .time.period,

                     stat.elas.s3bw   = calc.s3.speed,
                     stat.elas.effi   = .n.eff,
                     stat.elas.scan   = read.cache.mem + read.cache.sto
                     )
}

model.make.timing.fn <- function(.distr.list.caching,
                                 .distr.list.spooling,
                                 .max.count = NaN,
                                 .eff.fn = function(n) { 1 },
                                 .distr.caching.split.fn = model.distr.split.fn(TRUE),
                                 .time.period = 86400) {
  function(.query, .instances) {
    .scale <- c(1, .eff.fn(2:.max.count))
    .cdistr <- lapply(.distr.list.caching, .distr.caching.split.fn)
    .frames <- furrr::future_map_dfr(1:.max.count, function(i) {
      model.calc.time.for.config(
        .instances, i, .query, .cdistr[[i]], .distr.list.spooling[[i]], .scale[i], .time.period
      )
    })
  }
}

## ---------------------------------------------------------------------------------------------- ##
                                        # Data Preparation
## ---------------------------------------------------------------------------------------------- ##

model.calc.storage.speed <- function(.inst, network.speed) {
  bws <- purrr::map_dbl(
                  as.character(.inst$storage.type),
                  ~ model.factors.bandwidth[[.]]
                ) * .inst$id.slice.sto * .inst$id.slice.factor
  bws[is.na(bws)] = network.speed[is.na(bws)]
  bws
}

model.with.speeds <- function(inst) {
    dplyr::mutate(inst,
             calc.net.speed    = if_else(network.is.steady, network.Gbps, id.slice.net) / 8,
             calc.s3.speed     = calc.net.speed * 0.8,
             calc.mem.speed    = model.factors.bandwidth$RAM,
             calc.sto.speed    = model.calc.storage.speed(inst, calc.net.speed),
             ## no hyperthreads, a ssume 2 threads/core
             calc.cpu.real     = vcpu.count  / 2,
             calc.mem.caching  = memory.GiB  / 2,
             calc.sto.caching  = storage.GiB / 2,
             calc.mem.spooling = memory.GiB - calc.mem.caching,
             calc.sto.spooling = storage.GiB - calc.sto.caching,
           )
}

## ---------------------------------------------------------------------------------------------- ##
                                        # Calculate Prices
## ---------------------------------------------------------------------------------------------- ##

model.calc.costs <- function(query, inst, timing.fn) {
  if (nrow(inst) == 0) {
    print("No instances given, returning empty dataframe.")
    return(data.frame())
  }
  if (nrow(query) > 1) {
    results <- furrr::future_map_dfr(1:nrow(query), function(i) {
      times <- model.calc.costs(query[i, ], inst = inst, timing.fn = timing.fn)
      times$queryIndex <- i
      times
    })
    return(dplyr::group_by(results, queryIndex))
  }
  .inst <- inst %>% dplyr::filter(complete.cases(.)) %>% model.with.speeds()
  .times <- timing.fn(query, .inst)
  .stat.cols <- colnames(dplyr::select(.times, starts_with("stat.time.")))
  .times[, str_replace(.stat.cols, "stat.time", "stat.price")] <- .times[, .stat.cols] * .times$cost.usdph / 3600
  .times
}

model.recommend.from.timings <- function(query, timings, by = stat.price.sum) {
  by_var = enquo(by)
  timings %>%
    dplyr::top_n(-1, !! by_var) %>%
    dplyr::group_map(~ .$id) %>%
    purrr::map_chr(. %>% paste(collapse = ";"))
}

model.recommend.from.timings.arr <- function(query, timings, by = stat.price.sum) {
    by_var = enquo(by)
    timings %>%
        dplyr::top_n(-1, !! by_var) %>%
        dplyr::ungroup()
}

model.budgets.discrete <- function(.times, .budgets,
                                  .cost.fn = function(df) { df$stat.price.sum },
                                  .optim.fn = function(df) { df$stat.time.sum }) {
    furrr::future_map_dfr(.budgets, function(.budget) {
        .times %>%
            dplyr::mutate(
                       budget.limit = .budget,
                       budget.cost  = .cost.fn(.)
                   ) %>%
            dplyr::filter(budget.cost <= budget.limit) %>%
            dplyr::mutate(budget.optim = .optim.fn(.)) %>%
            dplyr::top_n(-1, wt = budget.optim) %>%
            dplyr::top_n(-1, wt = budget.cost)
    })
}

model.calc.frontier <- function(data, x = "x", y = "y", id = "id",
                                quadrant = c("top.right", "bottom.right",
                                             "bottom.left", "top.left"),
                                decreasing = TRUE) {
  if (!is.data.frame(data)) {
    stop(deparse(substitute(data)), " is not a data frame.")
  }

  z <- data[, c(x, y, id)]
  z <- stats::na.omit(z)

  if (!is.numeric(z[[x]]) | !is.numeric(z[[y]])) {
    stop("both x and y must be numeric variables")
  }

  quadrant <- match.arg(quadrant)
  if (quadrant == "top.right") {
    zz <- z[order(z[, 1L], z[, 2L], decreasing = TRUE), ]
    zz <- zz[which(!duplicated(cummax(zz[, 2L]))), ]
    zz[order(zz[, 1L], zz[, 2L], decreasing = decreasing), ]
  } else if (quadrant == "bottom.right") {
    zz <- z[order(z[, 1L], z[, 2L], decreasing = TRUE), ]
    zz <- zz[which(!duplicated(cummin(zz[, 2L]))), ]
    zz <- zz[which(!duplicated(zz[, 1L])), ]
    zz[order(zz[, 1L], zz[, 2L], decreasing = decreasing), ]
  } else if (quadrant == "bottom.left") {
    zz <- z[order(z[, 1L], z[, 2L], decreasing = FALSE), ]
    zz <- zz[which(!duplicated(cummin(zz[, 2L]))), ]
    zz[order(zz[, 1L], zz[, 2L], decreasing = decreasing), ]
  } else {
    zz <- z[order(z[, 1L], z[, 2L], decreasing = FALSE), ]
    zz <- zz[which(!duplicated(cummax(zz[, 2L]))), ]
    zz <- zz[order(zz[, 1L], zz[, 2L], decreasing = TRUE), ]
    zz <- zz[which(!duplicated(zz[, 1L])), ]
    zz[order(zz[, 1L], zz[, 2L], decreasing = decreasing), ]
  }
}

model.gen.workload <- function(p) {
  .distr.cache <- purrr::map(1:p$max.count, function(count) {
    model.make.distr.fn(p$cache.skew)(round(p$data.scan / count))
  })
  .distr.spool <- purrr::map(1:p$max.count, function(count) {
    model.make.distr.fn(p$spool.skew)(round(p$spool.frac * p$data.scan / count))
  })
  .time.fn <- model.make.timing.fn(
    .distr.list.caching  = .distr.cache,
    .distr.list.spooling = .distr.spool,
    .max.count = p$max.count,
    .eff.fn = model.make.scaling.fn(list(p = p$scale.fact)),
    .distr.caching.split.fn = model.distr.split.fn(p$load.first),
    .time.period = p$max.period
  )
  list(
    query = data.frame(
      time.cpu  = p$cpu.hours,
      data.read = p$data.scan
    ),
    time.fn = .time.fn
  )
}
