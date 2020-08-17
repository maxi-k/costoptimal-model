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

aws.data.historical.new.load <- function() {
  dates <- read.csv(paste(aws.data.folder, 'historical-data-times.csv', sep="/"))
  data <- read.csv(paste(aws.data.folder, 'historical-data-raw.csv', sep="/"))
  data <- sqldf("
    select *,
           case when storage_size is null then 'EBS'
                when storage_size = 0 then 'EBS'
                when storage_nvme_ssd = 'true' then 'NVMe'
                when storage_ssd = 'true' then 'SSD'
                else 'HDD'
           end as storage_type,
           substr(instance_type, 0, instr(instance_type, '.')) instance_prefix,
           substr(instance_type, instr(instance_type, '.')+1) instance_suffix,
           case when instr(physical_processor, 'AMD')!=0 then 'AMD'
                when instr(physical_processor, 'Intel')!=0 then 'Intel'
                when instance_type like 'a1%' then 'ARM'
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
             clockSpeed.text                   = clock_speed_ghz,
             region.name                       = "us-east-1",
             join.entry                        = entry - 1
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

aws.spot.price.history.load <- function(path = aws.data.folder, filename = "spotprices.csv") {
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
                   clock.ghz         = clockSpeed.value.ghz,
                   storage.GiB       = storage.sum.gib,
                   storage.count     = storage.count,
                   storage.type      = storage.type,
                   network.Gbps      = network_performance.value.Gib,
                   network.is.steady = network_performance.is_guaranteed,
                   cost.usdph        = cost.ondemand.value.usdph,
                   meta.region.name  = region.name,
                   meta.join.entry   = join.entry,
                   loading.comment      = ""
               ) %>%
        aws.data.cleanup() %>%
        dplyr::inner_join(
                   commits,
                   by = c("meta.join.entry" = "join.entry")
               )
}

aws.data.with.prefixes <- function(df) {
    df %>%
        dplyr::mutate(
                   id.prefix = sub("^([A-Za-z1-9-]+)\\..*", "\\1", id),
                   id.numstr = sub("^[A-Za-z1-9-]+\\.([1-9]*).*", "\\1", id),
                   id.number = id.numstr %>% as.numeric %>% replace(is.na(.), 0)
               ) %>%
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
    dplyr::arrange(desc(meta.join.entry, meta.origin)) %>%
    dplyr::group_by(meta.group)

aws.data.metacols <- c("meta.region.name", "meta.join.entry",
                       "meta.group", "meta.origin",
                       "commit.date", "commit.msg", "commit.hash")

aws.data.prefixes.irrelevant <- c(
  "a1", "g3", "p3", "g3s", "p2",
  "c1", "c3", "c4", "cr1", "d2",
  "u-9tb1", "u-12tb1", "u-6tb1", "u-24tb1",
  "r3", "m1", "m2", "m4", "m6g", "t1", "cc2", "m5a", "m5ad", "r5ad", "r5a"
)

## Filter functions

aws.data.filter.large <- function(df) {
    df %>%
        aws.data.with.prefixes() %>%
        dplyr::group_by(id.prefix) %>%
        dplyr::top_n(1, wt = id.number) %>%
        dplyr::ungroup()
}

aws.data.filter.relevant.family <- function(df) {
    df %>%
        aws.data.with.prefixes() %>%
        dplyr::filter(!(id.prefix %in% aws.data.prefixes.irrelevant))
}

## Precomputed filtered sets
aws.data.current.large <- aws.data.filter.large(aws.data.current)
aws.data.current.large.relevant <- aws.data.current %>%
    aws.data.filter.large() %>%
    aws.data.filter.relevant.family()
aws.data.current.relevant <- aws.data.filter.relevant.family(aws.data.current)

paper.inst.ids <- c(
 "c5n.18xlarge", "c5.24xlarge", "z1d.12xlarge", "c5d.24xlarge",
 "m5.24xlarge", "i3.16xlarge", "m5d.24xlarge", "m5n.24xlarge",
 "r5.24xlarge", "m5dn.24xlarge", "r5d.24xlarge", "r5n.24xlarge",
 "r5dn.24xlarge", "i3en.24xlarge", "x1e.32xlarge"
)

aws.data.filter.paper <- function(df) {
  dplyr::filter(df, id %in% paper.inst.ids)
}

aws.data.paper <- aws.data.filter.paper(aws.data.current)

## ---------------------------------------------------------------------------------------------- ##
                                        # SPOT INTERRUPTION FREQUENCIES
## ---------------------------------------------------------------------------------------------- ##

aws.spot.interruption.frequencies.load()

aws.spot.interruption.frequencies.plot <- function() {
    df <- aws.spot.interruption.frequencies %>%
        dplyr::group_by(instance.type) %>%
        dplyr::summarise(freq.avg = mean(freq.num), n = dplyr::n(), .groups = "drop")
    ggplot(df, aes(x = n, y = freq.avg, label = instance.type)) +
        geom_point() +
        geom_label_repel() +
        labs(title = "Interruption Frequencies",
             x = "Data points per Instance",
             y = "Avg Interruption Frequeny")
}

## Filter functions

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

## aws.spot.interruption.frequencies.plot()

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


## ---------------------------------------------------------------------------------------------- ##
                                        # DVASSALLO S3 MEASUREMENT DATA
## ---------------------------------------------------------------------------------------------- ##

s3.benchmark.dvassallo.raw.load()

s3.benchmark.dvassallo.data.max.obj.size <- s3.benchmark.dvassallo.raw.data %>%
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

## Precomputed sets
aws.data.current.s3join <- s3.benchmark.dvassallo.join(aws.data.current)
aws.data.current.large.s3join <- s3.benchmark.dvassallo.join(aws.data.current.large)

## ---------------------------------------------------------------------------------------------- ##
                                        # STYLES
## ---------------------------------------------------------------------------------------------- ##

style.instance.colored <- c(
  "c5", "c5n", "c5d", "m5", "i3",
)

## TODO: better palettes
## - c*, i3, m5
style.instance.colors <- styles.color.palette.light[1:length(style.instance.colored)]
names(style.instance.colors) <- style.instance.colored
