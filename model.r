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
    (1 - p$p) + (p$p / (p$eta * n))
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

model.calc.time.for.config <- function(.inst, .count, .query, .distr, .n.eff, .time.period) {
    .bins <- list(
        data.mem = list(size = round(.inst$memory.GiB),
                        prio = .inst$calc.memory.speed),
        data.sto = list(size = round(.inst$storage.GiB),
                        prio = .inst$calc.storage.speed),
        data.s3  = list(size = rep(length(.distr$working), times = nrow(.inst)),
                        prio = .inst$calc.network.speed)
    )
    .pack <- model.distr.pack(.bins, .distr$working)
    dplyr::transmute(.inst,
                     id.name          = id,
                     count            = .count,
                     id               = paste(id.name, count, sep="/"),
                     cost.usdph       = cost.usdph * .count,
                     # TODO: initial reads from s3?
                     read.mem         = .pack$data.mem,
                     read.sto         = .pack$data.sto,
                     read.s3          = .pack$data.s3,
                     read.xchg        = if_else(.count == 1, 0.0, .query$data.xchg * 1.0),
                     read.load        = sum(.distr$initial),
                     read.work        = sum(.distr$working),

                     stat.read.noxchg = read.mem + read.sto + read.s3 + read.load,
                     stat.read.sum    = stat.read.noxchg + read.xchg,

                     time.cpu         = .query$time.cpu * 3600, # h -> s
                     # time.mem         = read.mem   / calc.memory.speed,
                     time.sto         = read.sto   / calc.storage.speed,
                     time.s3          = read.s3    / calc.network.speed,
                     time.xchg        = read.xchg  / calc.network.speed,
                     time.load        = read.load  / calc.network.speed,

                     stat.time.sum    = (time.s3 + time.sto + time.cpu + time.xchg + time.load) * .n.eff,
                     stat.time.max    = pmax(time.s3, time.sto, time.cpu, time.xchg, time.load) * .n.eff,
                     stat.time.period = .time.period
                     )
}

model.make.timing.fn <- function(.distr.list,
                                 .max.count = NaN,
                                 .eff.fn = function(n) { 1 },
                                 .distr.split.fn = model.distr.split.fn(TRUE),
                                 .time.period = 86400) {
  function(.query, .instances) {
    .scale <- c(1, .eff.fn(2:.max.count))
    .distr <- lapply(.distr.list, .distr.split.fn)
    .frames <- furrr::future_map_dfr(1:.max.count, function(i) {
      model.calc.time.for.config(
        .instances, i, .query, .distr[[i]], .scale[i], .time.period
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
  dplyr::filter(inst, complete.cases(inst)) %>%
    dplyr::mutate(
             ## TODO discount based on 'up-to' -> split fair based on slices
             ##
             calc.network.factor = 1 - (!network.is.steady) * 0.6,
             calc.network.speed  = (network.Gbps / 8) * calc.network.factor,
             calc.memory.speed   = model.factors.bandwidth$RAM,
             calc.storage.speed  = model.calc.storage.speed(., calc.network.speed)
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
  .inst <- model.with.speeds(inst)
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
