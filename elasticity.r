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
    .cpu.factor <- (sum(sin(.window / .freqs)) + 1) / 2
    .mem.factor <- (sum(cos(.window / .freqs)) + 1) / 2
  dplyr::mutate(.w,
                time.cpu = time.cpu * .cpu.factor / .num,
                data.read = data.read * .mem.factor / .num
                )
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

                stat.price.sum    = stat.price.sum    + .recom.nxt$stat.price.sum,
                stat.price.max    = stat.price.max    + .recom.nxt$stat.price.max,
                stat.price.switch = 0 ,
                window.start      = .recom.cur$window.start,
                window.end        = .recom.nxt$window.start,
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
## )
model.elastic.costs <- function(.inst, .params) {
  .wl <- data.frame(
    time.cpu  = .params$time.cpu,
    data.read = .params$data.read
  )

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
                         lubridate::dseconds(.w$window.seconds))
  }

  # TODO: use result of previous merge for next window application (-> reduce)
  .mod <- model.modulate.workload(.wl, model.modulator.sin.other, .params$period.size, .params$window.size)
  slider::slide_dfr(.mod, .after = 1, function(.windows) {
    .cur <- head(.windows, n = 1)

    .costs.cur <- model.calc.costs(.cur, .inst, make.window.timing.fn(.cur))
    .recom.cur <- model.recommend.from.timings.arr(.cur, .costs.cur) %>%
      head(n = 1) %>%
      dplyr::mutate(window.start = .cur$window.id)

    if (nrow(.windows) == 1) {
      return(dplyr::mutate(.recom.cur, stat.price.switch = 0, window.end = .cur$window.id))
    }
    .nxt <- tail(.windows, n = 1)
    .costs.nxt <- model.calc.costs(.nxt, .inst, make.window.timing.fn(.nxt))
    .recom.nxt <- model.recommend.from.timings.arr(.cur, .costs.nxt) %>%
      head(n = 1) %>%
      dplyr::mutate(window.start = .nxt$window.id)
    if (.recom.cur$id == .recom.nxt$id) { #todo: multiple best?
      return(
        model.merge.window.costs(.recom.cur, .recom.nxt) %>%
        dplyr::mutate(stat.time.period = .cur$window.seconds + .nxt$window.seconds)
      )
    }
    .switch.cost <- .cur$data.read * .recom.nxt$stat.elas.effi / .recom.nxt$stat.elas.s3bw
    .stay.inst.cost <- dplyr::filter(.costs.nxt, id == .recom.cur$id) %>% head(n = 1)
    .stay.cost <- .stay.inst.cost$stat.price.sum - .recom.nxt$stat.price.sum
    return(
      if (.stay.cost <= .switch.cost) {
        model.merge.window.costs(.recom.cur, .stay.inst.cost) %>%
          dplyr::mutate(stat.time.period = .cur$window.seconds + .nxt$window.seconds)
      } else {
        .nxt.switch <- dplyr::mutate(.recom.nxt,
                                stat.price.switch = .switch.cost,
                                stat.price.sum    = stat.price.sum + stat.price.switch,
                                window.end        = .nxt$window.id)
        # TODO; reduce
        rbind(.recom.cur %>% dplyr::mutate(stat.price.switch = 0, window.end = .cur$window.id),
              .nxt.switch)
    })
  })
}

## --------------------------------------------------------------------------------
.params <- list(
  time.cpu    = 24 * 7 * 5,
  data.read   = 24 * 7 * 2000,
  cache.skew  = 0.1,
  spool.frac  = 0.1,
  spool.skew  = 0.8,
  scale.eff   = 0.9,
  max.inst    = 32,
  window.size = duration(2, "hour"),
  period.size = duration(1, "week")
)
.recoms <- model.elastic.costs(aws.data.current.large.relevant, .params)
## --------------------------------------------------------------------------------

plot.elastic.recoms <- function(.wl, .costs) {
  .maxval <- max(.wl$time.cpu)
  ggplot(.wl, aes(x = window.start)) +
    geom_line(aes(x = window.id, y = time.cpu,  color = "cpuh")) +
    geom_line(aes(x = window.id, y = data.read / 2000, color = "scan")) +
    # geom_line(data = .costs, aes(x = window.start, y = stat.price.sum, color = "$")) +
    geom_segment(data=.costs,
                 aes(x = window.start, xend = window.end,
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

ggsave("../figures/m5-elasticity-initial.pdf")
