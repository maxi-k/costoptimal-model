source("model.r")

model.modulate.workload <- function(.workload, .mod.fn, .duration, .window) {
  .num.windows <- floor(lubridate::seconds(.duration) / lubridate::seconds(.window))
  purrr::map_dfr(1:.num.windows, function(.window.id) {
    dplyr::mutate(.mod.fn(.workload, .window.id, .num.windows),
                  window.id = .window.id,
                  window.duration <- .window,
                  window.seconds <- lubridate::dseconds(.window)
                  )
  })
}

model.modulator.sin <- function(.w, .window, .num) {
  .factor <- (sin(.window / (2 * pi)) + 1)
  dplyr::mutate_all(.w, list(~ . * .factor / .num))
}

model.modulator.sin.other <- function(.w, .window, .num) {
  .freqs <- c(1, 1.5, 2, 3, 3.5, 4) * pi
  .factor <- max((sum(sin(.window / .freqs)) + 1) / 4, 0)
  dplyr::mutate(.w,
                time.cpu = time.cpu * .factor / .num,
                data.read = data.read * .factor / .num)
}

plot.modulated.workload <- function(.mod) {
  .df <- tidyr::pivot_longer(.mod, -window.id, names_to = "work.type", values_to = "value")
  ggplot(.df, aes(x = window.id, y = value, color = work.type)) + geom_line()
}

model.merge.window.costs <- function(.recom.cur, .recom.nxt) {
  dplyr::mutate(.recom.cur,  # TODO: implement better
                read.cache.load   = read.cache.load  + .recom.nxt$read.cache.load,
                read.cache.mem    = read.cache.mem   + .recom.nxt$read.cache.mem,
                read.cache.sto    = read.cache.sto   + .recom.nxt$read.cache.sto,
                read.cache.s3     = read.cache.s3    + .recom.nxt$read.cache.s3,

                read.spool.mem    = read.spool.mem   + .recom.nxt$read.spool.mem,
                read.spool.sto    = read.spool.sto   + .recom.nxt$read.spool.sto,
                read.spool.s3     = read.spool.s3    + .recom.nxt$read.spool.s3,

                rw.mem            = rw.mem           + .recom.nxt$rw.mem,
                rw.sto            = rw.sto           + .recom.nxt$rw.sto,
                rw.s3             = rw.s3            + .recom.nxt$rw.s3,
                rw.xchg           = rw.xchg          + .recom.nxt$rw.xchg,

                stat.read.spool   = stat.read.spool  + .recom.nxt$stat.read.spool,
                stat.read.work    = stat.read.work   + .recom.nxt$stat.read.work,

                time.cpu          = time.cpu         + .recom.nxt$time.cpu,
                time.mem          = time.mem         + .recom.nxt$time.mem,
                time.sto          = time.sto         + .recom.nxt$time.sto,
                time.s3           = time.s3          + .recom.nxt$time.s3,
                time.xchg         = time.xchg        + .recom.nxt$time.xchg,
                time.load         = time.load        + .recom.nxt$time.load,

                stat.time.sum     = stat.time.sum    + .recom.nxt$stat.time.sum,
                stat.time.max     = stat.time.max    + .recom.nxt$stat.time.max,
                stat.time.period  = stat.time.period + .recom.nxt$stat.time.period,
                stat.time.switch  = stat.time.switch,

                stat.price.sum    = stat.price.sum    + .recom.nxt$stat.price.sum,
                stat.price.max    = stat.price.max    + .recom.nxt$stat.price.max,
                stat.price.period = stat.price.period + .recom.nxt$stat.price.period,
                stat.price.switch = stat.price.switch,

                window.start      = .recom.cur$window.start,
                window.end        = .recom.nxt$window.start,

                stat.elas.s3bw   = .recom.nxt$stat.elas.s3bw,
                stat.elas.effi   = .recom.nxt$stat.elas.effi,
                stat.elas.scan   = .recom.nxt$read.cache.mem + .recom.nxt$read.cache.sto
  )
}

model.make.distrs <- function(.skew, .count, .read) {
  .dfn <- model.make.distr.fn(.skew)
  purrr::map(1:.count, function(.n) {
    .read.frac <- .read / .count
    if (.read.frac < 1) {
      0
    } else {
      .dfn(round(.read.frac))
    }
  })
}

model.scale.down.distrs <- function(.distrs, .factor) {
  purrr::map(.distrs, function(.dist) {
    .dist * .factor
  })
}

## params <- list(
##   time.cpu    = 24 * 7 * 20,
##   data.read   = 24 * 7 * 1000,
##   cache.skew  = 0.1,
##   spool.frac  = 0.1,
##   spool.skew  = 0.8,
##   scale.eff   = 0.9,
##   max.inst    = 32,
##   window.size = duration(1, "hour"),
##   period.size = duration(1, "week")
##   max.latency = duration(1, "hour")
## )
model.elastic.costs <- function(.inst, .params) {
  stopifnot(nrow(.inst) > 0)
  .wl <- data.frame(
    time.cpu  = .params$time.cpu,
    data.read = .params$data.read
  )
  .latency.sec <- int_length(.params$max.latency)
  .window.sec  <- int_length(.params$window.size)

  .dist.cache.whole <- model.make.distrs(.params$cache.skew, .params$max.inst, .params$data.read)
  .dist.spool.whole <- model.make.distrs(.params$cache.skew, .params$max.inst, .params$data.read * .params$spool.frac)
  .dist.split.fn <- model.distr.split.fn(FALSE)
  .scale.eff.fn  <- model.make.scaling.fn(list(p = .params$scale.eff))

  make.window.timing.fn <- function(.w) {
    .cache.dist <- model.scale.down.distrs(.dist.cache.whole, .w$data.read / .wl$data.read)
    .spool.dist <- model.scale.down.distrs(.dist.spool.whole, .w$data.read / .wl$data.read)
    model.make.timing.fn(.cache.dist,
                         .spool.dist,
                         .params$max.inst,
                         .scale.eff.fn,
                         .dist.split.fn,
                         .window.sec)
  }

  costs.for.window <- function(.w, .max.latency = .latency.sec) {
    possible <- model.calc.costs(.w, .inst, make.window.timing.fn(.w)) %>%
      dplyr::filter(stat.time.sum <= .max.latency) %>%
      dplyr::arrange(stat.price.period) %>%
      dplyr::mutate(window.start = .w$window.id,
                    window.end = .w$window.id,
                    stat.time.switch = 0,
                    stat.price.switch = 0)
  }
  best <- function(.w) head(.w, n = 1)

  .mod <- model.modulate.workload(.wl, model.modulator.sin.other, .params$period.size, .params$window.size)
  .split <- split(.mod, f = 1:nrow(.mod))
  .init <- list(best(costs.for.window(.split[[1]])))
  .stats <- list("same best" = 0, "switch" = 0, "stay" = 0, "phony" = 0)
  addstat <- function(name) {
    .stats[[name]] <<- .stats[[name]] + 1
  }
  res <- purrr::reduce(.split[2:length(.split)], .init = .init, function(.acc, .cur) {
    .recom.prv <- .acc[[length(.acc)]]
    stopifnot(.recom.prv$window.end + 1 == .cur$window.id)
    .costs.cur <- costs.for.window(.cur)
    if(nrow(.costs.cur) == 0) {
      stop("no instance satisfies time constraint")
    }
    if (.recom.prv$id == best(.costs.cur)$id) { # best stays the same
      addstat("same best")
      .acc[[length(.acc)]] <-
        dplyr::mutate(
                 model.merge.window.costs(.recom.prv, best(.costs.cur)),
                 window.start   = .recom.prv$window.start,
                 window.end     = .cur$window.id)
      return(.acc)
    } # best is different
    .time.switch <- (if (best(.costs.cur)$id.name == .recom.prv$id.name) {
                       .recom.prv$stat.elas.scan / .recom.prv$count
                     } else {
                       .recom.prv$stat.elas.scan
                     }) / (best(.costs.cur)$stat.elas.s3bw * best(.costs.cur)$count)
    .cost.switch <- .time.switch * best(.costs.cur)$cost.usdph / 3600
    .switch.candidate <- best(dplyr::filter(.costs.cur, stat.time.sum + .time.switch <= .window.sec))
    .can.switch <- nrow(.switch.candidate) > 0
    .can.stay <- .recom.prv$id %in% .costs.cur$id
    if(.can.stay) {
      .prv.incur <- .costs.cur %>% dplyr::filter(id == .recom.prv$id) %>% best()
      .cost.stay <- .prv.incur$stat.price.period - best(.costs.cur)$stat.price.period
      if (!.can.switch || .cost.stay < .cost.switch * (1 + .params$min.cost.frac)) {
        addstat("stay")
        .acc[[length(.acc)]] <- model.merge.window.costs(.recom.prv, .prv.incur)
        return(.acc)
      }
    }
    if (!.can.stay && !.can.switch) {
      warning("Can neither stay nor switch. Switching to the fastest instance.")
      addstat("phony")
      .switch.candidate <- .costs.cur %>% dplyr::arrange(stat.time.sum, stat.price.sum) %>% best()
      ## browser()
    }
    addstat("switch")
    .acc[[length(.acc) + 1]] <- .switch.candidate %>%
      dplyr::mutate(stat.time.switch = .time.switch,
                    stat.price.switch = .cost.switch)
    .acc
  }) %>% bind_rows()
  print(.stats)
  res
}

## --------------------------------------------------------------------------------
.params <- list(
  time.cpu    = 24 * 7 * 5,
  data.read   = 24 * 7 * 1000,
  cache.skew  = 0.1,
  spool.frac  = 0.1,
  spool.skew  = 0.8,
  scale.eff   = 0.9,
  max.inst    = 32,
  window.size = duration(1, "hour"),
  period.size = duration(1, "week"),
  max.latency = duration(0.5, "hour"),
  min.cost.frac = 0.3
)
.recoms <- model.elastic.costs(aws.data.current.relevant, .params)

local({
  .wl <- data.frame(
    time.cpu  = .params$time.cpu,
    data.read = .params$data.read
  )
  .dist.cache.whole <- model.make.distrs(.params$cache.skew, .params$max.inst, .params$data.read)
  .dist.spool.whole <- model.make.distrs(.params$cache.skew, .params$max.inst, .params$data.read * .params$spool.frac)
  .dist.split.fn <- model.distr.split.fn(FALSE)
  .scale.eff.fn  <- model.make.scaling.fn(list(p = .params$scale.eff))
  .time.fn <- model.make.timing.fn(.dist.cache.whole,
                                   .dist.spool.whole,
                                   .params$max.inst,
                                   .scale.eff.fn,
                                   .dist.split.fn,
                                   int_length(.params$period.size))
  .nwindows <- int_length(.params$period.size) / int_length(.params$window.size)
  .baseline.costs <- model.calc.costs(.wl, aws.data.current.relevant, .time.fn)
  .baseline.recom <- .baseline.costs %>%
    dplyr::filter(round(stat.time.sum) <= int_length(.params$max.latency) * .nwindows) %>%
    top_n(-1, wt = stat.price.sum) %>%
    head(n = 1)
  print(paste("Baseline with", .baseline.recom$id, "costs", .baseline.recom$stat.price.period))
  print(paste("Split workload costs ", sum(.recoms$stat.price.period)))
  .price.diff <- .baseline.recom$stat.price.period - sum(.recoms$stat.price.period)
  print(paste("Difference: ", .price.diff))
})
## --------------------------------------------------------------------------------

plot.elastic.recoms <- function(.wl, .costs) {
  .maxval <- max(.wl$time.cpu)
  ggplot(.wl, aes(x = window.start)) +
    geom_line(aes(x = window.id, y = time.cpu,  color = "cpuh")) +
    geom_line(aes(x = window.id, y = data.read / 2000, color = "scan")) +
    # geom_line(data = .costs, aes(x = window.start, y = stat.price.sum, color = "$")) +
    # geom_text(data=.costs, aes(label = str_replace(id, "xlarge", ""),
    #                            x = (window.start + window.end) / 2,
    #                            y = .maxval),
    #           angle = 90, size = 3) +
    geom_segment(data=.costs,
                 aes(x = window.start, xend = window.end + 1,
                     y = .maxval, yend = .maxval,
                     color = id), size = 3)
}

plot.elastic.recoms(
  model.modulate.workload(
    data.frame(
      time.cpu  = .params$time.cpu,
      data.read = .params$data.read
    ),
    model.modulator.sin.other, .params$period.size, .params$window.size),
  .recoms
)

write.csv(.recoms, "data/elasticity.data.csv")
ggsave("../figures/m5-elasticity-initial.pdf")
