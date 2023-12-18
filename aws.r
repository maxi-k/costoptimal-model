source("./util.r")

## ---------------------------------------------------------------------------------------------- ##
                                        # LOADING & FILES
## ---------------------------------------------------------------------------------------------- ##

aws.data.folder <- "./data"

aws.data.coltypes <- cols(
    vcpu.value.count                  = col_number(),
    memory.value.gib                  = col_number(),
    clockSpeed.value.ghz              = col_number(),
    storage.sum.gib                   = col_number(),
    storage.count                     = col_number(),
    network_performance.value.Gib     = col_number(),
    network_performance.is_guaranteed = col_logical(),
    cost.ondemand.value.usdph         = col_number(),
    processorName                     = col_character(),
    clockSpeed.text                   = col_character(),
    region.name                       = col_character(),
    join.entry                        = col_number()
)

aws.data.commits.coltypes <- cols(
    join.entry  = col_number(),
    commit.hash = col_character(),
    commit.date = col_character(),
    commit.msg  = col_character()
)

aws.data.historical.load <- function() {
    aws.data.historical <<- aws.data.load(filename = "historical-data.csv")
}

fractional.year.to.date <- function(date) {
  ## convert date number 'year.yeardate', e.g. 2023.250
  ## to an actual fraction of the year, e.g. 2023.(250/365)
  year <- paste(substring(date, 1, 4), "-01-01", sep="")
  daysofyear <- substring(date, 6)
  as.Date(year) + days(daysofyear) - days(1)
}

aws.data.historical.new.load <- function() {
  dates <- read.csv(paste(aws.data.folder, 'historical-data-times.csv', sep="/"), colClasses = c("numeric", "Date"))
  data <- read.csv(paste(aws.data.folder, 'historical-data-raw.csv', sep="/"))
  data <- sqldf("
    select *,
           case when storage_size is null then 'EBS'
                when storage_size = 0 then 'EBS'
                when storage_nvme_ssd = 'true' and storage_ssd = 'true' then 'NVMe'
                when storage_ssd = 'true' then 'SSD'
                else 'HDD'
           end as storage_type,
           substr(instance_type, 0, instr(instance_type, '.')) instance_prefix,
           substr(instance_type, instr(instance_type, '.')+1) instance_suffix,
           case when instr(physical_processor, 'AMD')!=0 then 'AMD'
                when instr(physical_processor, 'Intel')!=0 then 'Intel'
                when instance_type like 'a1%' then 'ARM'
                when physical_processor like '%Graviton%' then 'ARM'
                else '?'
           end as CPU_brand
    from data d1 join dates using (entry)
    where pricing is not null
    and vCPU is not null
    and generation = 'current'
    and network_performance NOT IN ('High', 'Moderate', 'Low', 'Very Low', 'Very High')
    and (GPU is null or GPU = 0)
    and (FPGA is null or FPGA = 0)
    and instance_type not like 'a1%' --ARM
    and instance_type not like '%.metal'
    and instance_type not like 't2%' --burst
    and instance_type not like 't3%' --burst")
  aws.data.historical.new <<- as.data.frame(data) %>%
    dplyr::transmute(
             id                                = as.character(instance_type),
             vcpu.value.count                  = as.numeric(as.character(vCPU)),
             memory.value.gib                  = as.numeric(memory),
             clockSpeed.value.ghz              = as.numeric(str_replace(clock_speed_ghz, "GHz", "")),
             clockSpeed.value.ghz              = ifelse(clockSpeed.value.ghz == "", NULL, clockSpeed.value.ghz),
             clockSpeed.value.ghz              = as.numeric(clockSpeed.value.ghz),
             storage.sum.gib                   = storage_size * storage_devices,
             storage.sum.gib                   = ifelse(is.na(storage.sum.gib), 0, storage.sum.gib),
             storage.count                     = ifelse(is.na(storage_devices), 0, storage_devices),
             storage.type                      = storage_type,
             network_performance               = network_performance,
             network_performance.value.Gib     = str_replace(network_performance, "Up to", ""),
             network_performance.value.Gib     = str_replace(network_performance.value.Gib, "Gigabit", ""),
             network_performance.value.Gib     = str_replace(network_performance.value.Gib, "Gbps", ""),
             network_performance.value.Gib     = as.numeric(network_performance.value.Gib),
             network_performance.is_guaranteed = !str_detect(network_performance, "Up to"),
             cost.ondemand.value.usdph         = as.numeric(pricing),
             processorName                     = physical_processor,
             processorBrand                    = CPU_brand,
             clockSpeed.text                   = clock_speed_ghz,
             region.name                       = "us-east-1",
             join.entry                        = entry - 1,
             join.time                         = time
           )
  aws.data.historical.new
}

aws.data.commits.load <- function(path = aws.data.folder,
                                  filename = "ec2-instances.info-commit-mapping.csv") {
    full.path <- paste(path, filename, sep="/")
    result <- read_csv(full.path, col_types = aws.data.commits.coltypes)
    aws.data.commits <<- result
    result
}

aws.spot.interruption.frequencies.coltypes <- cols(
    instance.type         = col_character(),
    vCPU                  = col_number(),
    memory                = col_number(),
    savings.over.ondemand = col_character(),
    freq.text             = col_character(),
    region.name           = col_character(),
    region.id             = col_character(),
    freq.num              = col_number()
)

aws.spot.interruption.frequencies.load <- function(path = aws.data.folder,
                                                   filename = "interruption-frequencies.csv") {
    full.path <- paste(path, filename, sep="/")
    result <- read_csv(full.path, col_types = aws.spot.interruption.frequencies.coltypes)
    aws.spot.interruption.frequencies <<- result
    result
}

aws.spot.price.history.load <- function(path = aws.data.folder, filename = "spotprices-us-east-1.csv") {
    full.path <- paste(path, filename, sep="/")
    result <- read_csv(full.path)
    aws.spot.price.history <<- result
    result
}

s3.benchmark.dvassallo.raw.load <- function(path = aws.data.folder,
                                            filename = "dvassallo-s3-benchmark-github.csv") {
    file.path <- paste(path, filename, sep = "/")
    s3.benchmark.dvassallo.raw.path <<- file.path

    s3.benchmark.dvassallo.raw.data <<- read_csv(
        file.path,
        col_types = cols(
            id               = col_character(),
            object.size      = col_number(),
            thread.count     = col_number(),
            thread.outliers  = col_logical(),
            net.bandwidth.mb = col_number()
        )

    )
}

aws.data.commits.load()
aws.data.historical.new.load()

## ---------------------------------------------------------------------------------------------- ##
                                        # Data Processing
## ---------------------------------------------------------------------------------------------- ##

aws.data.cleanup <- function(df) {
    mk.default <- function(df, col, val, msg) {
        col_var = enquo(col)
        df %>% dplyr::mutate(
                          loading.comment = ifelse(is.na(!!col_var),
                                         ifelse(loading.comment == "", msg, paste(loading.comment, msg, sep=". ")),
                                         loading.comment),
                          !!col_var := ifelse(is.na(!!col_var), val, !!col_var)
                      )
    }
    df %>% mk.default(clock.ghz, 2.5, "Clock speed unkown, assuming default value of 2.5 GHz")
}

aws.data.normalize <- function(df, commits = aws.data.commits) {
    df %>%
        dplyr::transmute(
                   id                = id,
                   memory.GiB        = memory.value.gib,
                   vcpu.count        = vcpu.value.count,
                   cpu.brand         = processorBrand,
                   clock.ghz         = clockSpeed.value.ghz,
                   storage.GiB       = storage.sum.gib,
                   storage.count     = storage.count,
                   storage.type      = storage.type,
                   network.Gbps      = network_performance.value.Gib,
                   network.is.steady = network_performance.is_guaranteed,
                   cost.usdph        = cost.ondemand.value.usdph,
                   meta.region.name  = region.name,
                   meta.join.entry   = join.entry,
                   meta.join.time    = join.time,
                   loading.comment      = ""
               ) %>%
        aws.data.cleanup() %>%
        dplyr::inner_join(
                   commits,
                   by = c("meta.join.entry" = "join.entry")
               )
}

aws.data.enhance.ids <- function(df) {
  dplyr::mutate(df,
                id.prefix = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id),
                id.numstr = sub("^[A-Za-z1-9-]+\\.([1-9]*).*", "\\1", id),
                id.number = id.numstr %>% as.numeric %>% replace(is.na(.), 0)
                )
}

aws.data.with.prefixes <- function(df) {
  df %>%
    aws.data.enhance.ids() %>%
    dplyr::group_by(id.prefix) %>%
    dplyr::group_modify(function(df, group) {
      large <- top_n(df, 1, wt = id.number)
      dplyr::mutate(df,
                    id.slice        = if_else(id.number == 0,
                                              if_else(str_detect(id, "metal"), large$id.number,
                                                      if_else(str_detect(id, "xlarge"), 1, 0.5)),
                                              id.number),
                    id.slice.factor = id.slice / large$id.number,
                    id.slice.of     = large$id,
                    id.slice.net    = large$network.Gbps * id.slice.factor,
                    id.slice.sto    = large$storage.count) }) %>%
    dplyr::ungroup()
}


aws.data.all <-
  aws.data.historical.new %>%
  aws.data.normalize() %>%
  dplyr::group_by(meta.join.entry) %>%
  dplyr::group_modify(function(df, g) { aws.data.with.prefixes(df); }) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(meta.origin = "instances.json")

aws.data.current <- dplyr::filter(aws.data.all, meta.join.entry == max(meta.join.entry))

aws.data.all.by.date <- aws.data.all %>%
    dplyr::mutate(
               meta.group = paste(commit.date, meta.join.entry, meta.origin, sep = " | ")
           ) %>%
    dplyr::arrange(desc(meta.join.entry), desc(meta.origin)) %>%
    dplyr::group_by(meta.group)

aws.data.metacols <- c("meta.region.name", "meta.join.entry",
                       "meta.group", "meta.origin",
                       "commit.date", "commit.msg", "commit.hash")

aws.data.prefixes.irrelevant <- c(
  "a1", "g3", "p3", "g3s", "p2",
  "c1", "c3", "c4", "cr1", "d2",
  "u-9tb1", "u-12tb1", "u-6tb1", "u-24tb1",
  "hpc7g",
  "r3", "m1", "m2", "m4", "t1", "cc2", "m5a", "m5ad", "r5ad", "r5a" # "m6g"
)

## Filter functions

aws.data.filter.large <- function(df) {
    df %>%
        aws.data.enhance.ids() %>%
        dplyr::group_by(id.prefix) %>%
        dplyr::top_n(1, wt = id.number) %>%
        dplyr::ungroup()
}

aws.data.filter.large2 <- function(df) {
    df %>%
        dplyr::group_by(id.prefix) %>%
        dplyr::top_n(1, wt = id.number) %>%
        dplyr::ungroup()
}

aws.data.filter.small2 <- function(df) {
    dplyr::anti_join(df, aws.data.filter.large2(df), "id")
}

aws.data.filter.relevant.family <- function(df) {
    df %>%
      aws.data.enhance.ids() %>%
      dplyr::filter(!(id.prefix %in% aws.data.prefixes.irrelevant))
}

aws.data.filter.relevant.family2 <- function(df) {
    dplyr::filter(df, !(id.prefix %in% aws.data.prefixes.irrelevant))
}

## Only large and new data; commonly used -> precompute
aws.data.current.large <- aws.data.filter.large(aws.data.current)

paper.inst.ids <- c(
 "c5n.18xlarge", "c5.24xlarge", "z1d.12xlarge", "c5d.24xlarge",
 "m5.24xlarge", "i3.16xlarge", "m5d.24xlarge", "m5n.24xlarge",
 "r5.24xlarge", "m5dn.24xlarge", "r5d.24xlarge", "r5n.24xlarge",
 "r5dn.24xlarge", "i3en.24xlarge", "x1e.32xlarge"
)

aws.data.filter.paper <- function(df) {
  dplyr::filter(df, id %in% paper.inst.ids)
}

## ---------------------------------------------------------------------------------------------- ##
                                        # SPOT INTERRUPTION FREQUENCIES
## ---------------------------------------------------------------------------------------------- ##

aws.spot.interruption.frequencies.load()

aws.data.mkfilter.spot.inter.freq <- function(perc) {
    function(df) {
        if(nrow(df) == 0) {
            return(df);
        }
        .region <- head(df, n = 1)$meta.region.name
        freq <- aws.spot.interruption.frequencies %>%
            dplyr::filter(region.id == .region) %>%
            dplyr::group_by(instance.type) %>%
            dplyr::summarise(meta.freq.avg = mean(freq.num), .groups = "drop") %>%
            dplyr::filter(meta.freq.avg <= perc)
        dplyr::inner_join(df, freq, by = c("id" = "instance.type"))
    }
}

## ---------------------------------------------------------------------------------------------- ##
                                        # SPOT PRICES
## ---------------------------------------------------------------------------------------------- ##

aws.spot.price.history.averages <- aws.spot.price.history.load() %>%
    dplyr::group_by(InstanceType) %>%
    dplyr::summarize(cost.usdph.avg = mean(SpotPrice),
                     meta.time.min = min(Timestamp),
                     meta.time.max = max(Timestamp)) %>%
    dplyr::rename(id = InstanceType)

## Filter functions

aws.data.filter.spot.price <- function(df) {
    df %>%
        dplyr::inner_join(aws.spot.price.history.averages, by = "id") %>%
        dplyr::mutate(cost.usdph = cost.usdph.avg) %>%
        dplyr::select(-cost.usdph.avg)
}

aws.data.filter.spot.price.inter.freq <- function(df, freq = 5) {
  freq.filter <- aws.data.mkfilter.spot.inter.freq(freq)
  spot <- df %>%
    freq.filter() %>%
    aws.data.filter.spot.price() %>%
    dplyr::select(-meta.time.max, -meta.time.min, -meta.freq.avg)
  rbind(
    dplyr::anti_join(df, spot, by = "id") %>% dplyr::mutate(meta.uses.spot.price = FALSE),
    spot %>% dplyr::mutate(meta.uses.spot.price = TRUE)
  )
}

## ---------------------------------------------------------------------------------------------- ##
                                        # DVASSALLO S3 MEASUREMENT DATA
## ---------------------------------------------------------------------------------------------- ##

s3.benchmark.dvassallo.data.max.obj.size <- s3.benchmark.dvassallo.raw.load() %>%
    dplyr::group_by(id) %>%
    dplyr::top_n(1, wt = object.size) %>%
    dplyr::mutate(
               net.bench.Gbps = net.bandwidth.mb * 0.008
           ) %>%
    dplyr::summarize(
               net.bench.Gbps.mean = mean(net.bench.Gbps),
               net.bench.Gbps.min  = min(net.bench.Gbps),
               net.bench.Gbps.max  = max(net.bench.Gbps),
               net.bench.Gbps.sd   = sd(net.bench.Gbps)
           )

s3.benchmark.dvassallo.join <- function(instance, benchmark = s3.benchmark.dvassallo.data.max.obj.size) {
    dplyr::inner_join(instance, benchmark, by = "id") %>%
        dplyr::mutate(network.Gbps = net.bench.Gbps.mean)
}


## Filter functions
aws.data.filter.s3join.all <- function(df) {
    s3.benchmark.dvassallo.join(df)
}

aws.data.filter.s3join.large <- function(df) {
    df %>%
        aws.data.filter.large() %>%
        s3.benchmark.dvassallo.join()
}

## ---------------------------------------------------------------------------------------------- ##
                                        # PRECOMPUTED SETS
## ---------------------------------------------------------------------------------------------- ##

## Precompute data for plots / faster computation
## only do it when not deployed to shiny to preserve memory
if (!util.is.shiny.deployed) {

  ## newest (2020) data
  aws.data.current.large.relevant <- aws.data.current %>%
    aws.data.filter.large() %>%
    aws.data.filter.relevant.family()

  ## only large instances
  aws.data.current.relevant <- aws.data.filter.relevant.family(aws.data.current)

  ##
  ## interruption frequency data
  ##

  aws.data.current.large.relevant.spot.lt5 <- aws.data.current.large.relevant %>% aws.data.filter.spot.price.inter.freq()
  aws.spot.interruption.frequencies %>%
    rename(id = instance.type) %>%
    aws.data.filter.large() %>%
    aws.data.filter.relevant.family() %>%
    filter(freq.num == 5, region.id == "us-east-1") %>%
    select(id, freq.text)

  ##
  ## Instance data reffered to in the costoptimal paper
  ##
  aws.data.paper <- aws.data.filter.paper(aws.data.current)

  ##
  ## different views on spot price history data
  ##

  aws.data.spot.by.date.az <- aws.spot.price.history %>%
    dplyr::mutate(parsed.date = lubridate::round_date(Timestamp, unit = "hour")) %>%
    dplyr::select(-Timestamp, -ProductDescription) %>%
    dplyr::group_by(parsed.date, InstanceType) %>%
    dplyr::rename(id = InstanceType) %>%
    dplyr::arrange(parsed.date)

  aws.data.spot.by.date <- aws.spot.price.history %>%
    dplyr::mutate(parsed.date = lubridate::round_date(Timestamp, unit = "hour")) %>%
    dplyr::filter(AvailabilityZone == "us-east-1a") %>%
    dplyr::select(-Timestamp, -ProductDescription, -AvailabilityZone) %>%
    dplyr::rename(id = InstanceType) %>%
    dplyr::arrange(parsed.date)

  aws.data.spot.min.date <- min(aws.data.spot.by.date$parsed.date) - lubridate::days(1)

  aws.data.spot.filled <- aws.data.spot.by.date %>%
    dplyr::group_by(parsed.date) %>%
    dplyr::group_split() %>%
    purrr::reduce(.init = setNames(list(dplyr::transmute(aws.data.current,
                                                         SpotPrice = cost.usdph,
                                                         id = id,
                                                         ## AvailabilityZone = "eu-central-1a",
                                                         parsed.date = aws.data.spot.min.date)),
                                   aws.data.spot.min.date),
                  function(acc, row) {
                    prev <- acc[[length(acc)]]
                    diff <- dplyr::anti_join(prev, row, "id") %>%
                      dplyr::mutate(parsed.date = head(row, n = 1)$parsed.date)
                    acc[[length(acc) + 1]] <- rbind(row, diff)
                    acc
                  }) %>%
    bind_rows() %>%
    group_by(parsed.date)

  aws.data.spot.joined <- dplyr::inner_join(aws.data.current, aws.data.spot.filled, by = "id")

  aws.data.spot.with.intfreq <- aws.data.spot.joined %>%
    aws.data.mkfilter.spot.inter.freq(5)() %>%
                                       dplyr::mutate(
                                                cost.usdph = SpotPrice,
                                                uses.spot.price = TRUE) %>%
                                       dplyr::select(-meta.freq.avg) %>%
                                       rbind(
                                         dplyr::anti_join(aws.data.spot.joined, ., by = "id") %>% dplyr::mutate(uses.spot.price = FALSE),
                                         .
                                       )

  ##
  ## s3 benchmark data
  ##
  aws.data.current.s3join <- s3.benchmark.dvassallo.join(aws.data.current)
  aws.data.current.large.s3join <- s3.benchmark.dvassallo.join(aws.data.current.large)
}

## ---------------------------------------------------------------------------------------------- ##
                                        # STYLES
## ---------------------------------------------------------------------------------------------- ##

style.intel.colors <- c(
  # + m3 plot
  "c5n"  = "#b48ead",
  "c5d"  = "#a3be8c",
  "m5n"  = "#81a1c1",
  "r5n"  = "#8fbcbb",
  # + m2 plot
  "c5"   = "#5e81ac",
  "i3"   = "#ebcb8b",
  "m5"   = "#4c566a",
  "m5d"  = "#bf616a",
  "m5dn" = "#d08770",
  "r5"   = "#88c0d0",
  "r5n"  = "#d8dee9",
  # + m1-all plot
  "i3en" = "#d8dee9",
  # + m3 plot
  "c5d.2" = "#5e81ac",
  # new instances
  "c6in" = "#d08770",
  "u-18tb1" = "#ebcb8b",
  "m6a"  = "#ebcb8b"
)
arm.color.delta <- 0.1
style.arm.colors <- c(
  "c6gn"  = "#b48ead",
  "c6g"   = "#a3be8c",
  "c7gn"  = "#81a1c1",
  "c7g"   = "#8fbcbb",
  "m6g"   = "#d08770",
  "r6g"  = "#d8dee9"
) |> sapply(shades::brightness, delta(arm.color.delta)) |> sapply(shades::saturation, delta(arm.color.delta/2))
style.instance.colors <- c(style.intel.colors, style.arm.colors)
style.instance.colored <- names(style.instance.colors)
style.instance.colors.vibrant <- as.character(shades::saturation(style.instance.colors, delta(0.5)))
names(style.instance.colors.vibrant) <- style.instance.colored

## styles.draw.palette(style.instance.colors.vibrant)
