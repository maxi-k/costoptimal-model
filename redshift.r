source("./model.r")
source("./aws.r")

redshift.ods.path <- "data/redshift.ods"
redshift.raw <- read_ods(redshift.ods.path)
redshift.df <- transmute(redshift.raw,
    gib                  = gib,
    rows                 = gib * 1024^3 / 16,
    unique               = unique,
    unique.frac          = unique / rows,
    query.sec            = query_sec,
    unload.sec           = unload_sec,
    keys.flag            = keys,
    has.key.primary      = keys != 0,
    has.key.distribution = keys == 2,
    scaling              = 2,
    instance             = "ra3.4xlarge"
  ) %>% filter(!is.na(unload.sec))

redshift.instance <- data.frame(
  id = "ra3.4xlarge",
  memory.GiB = 96,
  storage.GiB = 0, # TODO is this true? can't find data on this
  network.Gbps = 2 * 8,
  clock.ghz = 3, # TODO is this true?
  vcpu.count = 12,
  network.is.steady = TRUE,
  storage.type = "NVMe",
  storage.count = 1, # TODO is this true?
  cost.usdph = 3.894,
  meta.region.name = "eu-central-1"
) %>% aws.data.with.prefixes()


redshift.costs <- slider::slide_dfr(filter(redshift.df, keys.flag == 0), function(row) {
  workload <- model.gen.workload(
    list(
      cpu.hours  = (row$gib / 3600), # TODO find out how to measure this
      data.scan  = row$gib,
      max.count  = 2,
      cache.skew = 0.0000001, # only single accesses
      spool.skew = 0.0000001, # only single accesses
      spool.frac = row$unique.frac,
      scale.fact = 1, # find out based on distribution key
      load.first = FALSE,
      max.period = 2^30
    )
  )
  model.calc.costs(workload$query, redshift.instance, workload$time.fn) %>%
    mutate(time.predicted = stat.time.sum,
           time.measured = row$unload.sec,
           unique.frac = row$unique.frac,
           gib = row$gib,
           label = paste(gib, "GiB /", signif(unique.frac * 100, 2), "%")
           ) %>% filter(count == 2)
})

ggplot(redshift.costs, aes(x = time.predicted, y = time.measured)) +
  geom_abline(color = "darkgrey") +
  geom_text_repel(aes(label = label)) +
  # scale_y_continuous(breaks = c(1,2,3,10,15,20,30)) +
  geom_point()
