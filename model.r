source("./util.r")

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
    if (length(size) == 0) {
      return(c())
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

    .pack.cache <- model.distr.pack(.bins.cache, .distr.cache$working)
    .pack.spool <- model.distr.pack(.bins.spool, .distr.spool)
    .spool.sum  <- sum(.distr.spool)
    .inv.eff <- .count * .n.eff

    dplyr::transmute(.inst,
                     id.name          = id,
                     count            = .count,
                     id               = paste(id.name, count, sep="/"),
                     cost.usdph       = cost.usdph * .count,

                     read.cache.load  = sum(.distr.cache$initial),
                     read.cache.mem   = .pack.cache$data.mem,
                     read.cache.sto   = .pack.cache$data.sto,
                     read.cache.s3    = .pack.cache$data.s3,

                     read.spool.mem   = .pack.spool$data.mem,
                     read.spool.sto   = .pack.spool$data.sto,
                     read.spool.s3    = .pack.spool$data.s3,

                     rw.mem           = read.cache.mem + 2 * read.spool.mem,
                     rw.sto           = read.cache.sto + 2 * read.spool.sto,
                     rw.s3            = read.cache.s3  + 2 * read.spool.s3,
                     rw.xchg          = if_else(.count == 1, 0.0, 2 * .spool.sum),

                     stat.read.spool  = .spool.sum,
                     stat.read.work   = sum(.distr.cache$working),

                     time.cpu         = (.query$time.cpu * 3600 / calc.cpu.real) * .n.eff,
                     # time.mem       = ( rw.mem         / calc.memory.speed)   * .n.eff,
                     time.sto         = (rw.sto          / calc.sto.speed) * .inv.eff,
                     time.s3          = (rw.s3           / calc.net.speed) * .inv.eff,
                     time.xchg        = (rw.xchg / 2     / calc.net.speed) * .inv.eff,
                     time.load        = (read.cache.load / calc.net.speed) * .inv.eff,

                     stat.time.sum    = time.s3 + time.sto + time.cpu + time.xchg + time.load,
                     stat.time.max    = pmax(time.s3, time.sto, time.cpu, time.xchg, time.load),
                     stat.time.period = .time.period
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
                ) * .inst$storage.count
  bws[is.na(bws)] = network.speed[is.na(bws)]
  bws
}

model.with.speeds <- function(inst) {
    dplyr::mutate(inst,
             ## TODO discount based on 'up-to' -> split fair based on slices
             calc.net.speed    = if_else(network.is.steady, network.Gbps, id.slice.net) / 8,
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
