source("./model.r")
source("./aws.r")

try.params <- function() {
  .range.scan  <- c(10    , 100 , 1000 , 10000, 100 * 1000)
  .range.cache <- c(0.001 , 0.1 , 0.5  , 0.8  , 0.999)
  .range.sdist <- c(0.001 , 0.5 ,  0.999)
  .range.spool <- c(0     , 0.1 , 0.5  , 0.8  , 1)
  .range.cpu   <- c(1)

  .split.fn <- model.distr.split.fn(FALSE)
  .insts <- aws.data.current.large.relevant

  purrr::map_dfr(.range.scan, function(.scanned) {
    purrr::map_dfr(.range.cache, function(.cache.skew) {
      .cache.distr <- model.make.distr.fn(.cache.skew)(.scanned)
      .cache.distr.list <- list(.cache.distr)
      purrr::map_dfr(.range.sdist, function(.spool.skew) {
        purrr::map_dfr(.range.spool, function(.spool.frac) {
          .spooled <- round(.spool.frac * .scanned)
          .spool.distr <- if(.spooled < 1) { 0 } else { model.make.distr.fn(.spool.skew)(.spooled) }
          .spool.distr.list <- list(.spool.distr)
          purrr::map_dfr(.range.cpu, function(.cpuh) {

            .query <- data.frame(
              time.cpu  = .cpuh,
              data.read = .scanned
            )
            .timer <- model.make.timing.fn(
              .distr.list.caching  = .cache.distr.list,
              .distr.list.spooling = .spool.distr.list,
              .max.count <- 1,
              .distr.caching.split.fn = .split.fn
            )

            .timer <- model.make.timing.fn(
              .distr.list.caching  = .cache.distr.list,
              .distr.list.spooling = .spool.distr.list,
              .max.count <- 1,
              .distr.caching.split.fn = .split.fn
            )
            .times <- model.calc.costs(.query, .insts, timing.fn = .timer)
            .recom <- .times %>% dplyr::top_n(-2, stat.price.sum) %>%
              dplyr::mutate(
                       is.first = stat.price.sum == min(stat.price.sum)
                     ) %>%
              dplyr::ungroup()
            dplyr::mutate(.recom,
                          param.cpuh = .cpuh,
                          param.scanned = .scanned,
                          param.cache.skew = .cache.skew,
                          param.spool.skew = .spool.skew,
                          param.spool.frac = .spool.frac
                          )
          })
        })
      })
    })
  })
}

system.time({
  res <<- try.params()
  write.csv(res, "tested.params.relevant.csv")
})

library("sqldf")
result <- sqldf("SELECT id, count(*) from res group by id")
result
