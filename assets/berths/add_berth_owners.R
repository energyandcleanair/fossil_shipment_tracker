

fixnames <- function(x) {
  x %>% stringi::stri_trans_general("Latin-ASCII") %>%
    gsub(' $', '', .) %>% gsub('Refinery\\.', 'Refinery', .) %>% gsub('Termials', 'Terminals', .)
}

disambiguate = function(x, keys, unique.values=F, match.all=F, stop.error=F, warn.error=F) {
  if(is.null(names(keys))) names(keys)=keys
  for(k in keys) {
    m=grep(k, x)
    hits=length(m)

    if(hits != 1) {
      msg=paste(k,'matched',hits, 'times')
      if(hits==0 & match.all) {
        if(stop.error) stop(smg)
        if(warn.error) warning(msg)
      }

      if(hits>1 & unique.values) {
        if(stop.error) stop(smg)
        if(warn.error) warning(msg)
      }
      print(msg)
    }

    x[m] <- names(keys)[keys==k]
  }
  return(x)
}

read_gcpt = function(GCPTver = "2022H1", #database version to use
                     GCPTpath = "data/") {

  files <- list(#f2015 = "Global Coal Plant Tracker January 2016.xlsx",
    f2016H2 = "Global Coal Plant Tracker Feb 2017c - China status.xlsx",
    f2017H1 = "Global Coal Plant Tracker July 2017a.xlsx",
    f2017H2 = "Bloomberg Jan 2018 GCPT.xlsx",
    f2018H1 = "Global Coal Plant Tracker July 2018a.xlsx",
    f2018H2 = "Global Coal Plant Tracker January 2019.xlsx",
    f2019H1 = "Global Coal Plant Tracker July 2019_12July.xlsx",
    f2019H2 = "January 2020 Global Coal Plant Tracker.xlsx",
    f2020H1 = 'July 2020 Global Coal Plant Tracker.xlsx',
    f2020H2 = 'January 2021 Global Coal Plant Tracker.xlsx',
    f2021H1 = 'July 2021 Global Coal Plant Tracker.xlsx',
    f2022H1 = 'Global-Coal-Plant-Tracker-Jan-2022.xlsx')

  infile = paste0(GCPTpath, '/', files[[paste0('f', GCPTver)]])
  insheet = intersect(excel_sheets(infile), c('Projects', 'Coal units', 'Units'))

  units <- read_xlsx(infile, sheet = insheet, guess_max = Inf)

  #fix an error where both coords are input into latitude column separated by comma...
  if(is.character(units$Latitude)) {
    llcols = c('Latitude', 'Longitude')
    units %>% dplyr::select(Latitude) %>% separate(Latitude, llcols, ',') %>%
      mutate_all(as.numeric) -> ll
    units %<>% mutate_at(llcols, as.numeric)
    ind = which(is.na(units$Longitude))
    units[ind,llcols] <- ll[ind, ]
  }

  #housekeeping
  units$Status %<>% tolower %>% gsub(" *$","",.) %>% as.factor
  units %>% mutate_at(vars(starts_with('Capaci'),
                           ends_with('itude')),
                      as.numeric)
}

getBerthData <- function(version='v7') {
  paste0('berth_infos.xlsx') %>%
    readxl::read_xlsx(.name_repair = make.names) %>%
    filter(!is.na(Polygon.name)) -> berthdata

  berthdata$Polygon.name %<>% fixnames

  na.cover <- function(x, x.new) { ifelse(is.na(x), x.new, x) }

  read_gcpt() %>% distinct(GEM.coal.power.TrackerLOC=TrackerLOC, Parent) %>%
    left_join(berthdata, .) %>% mutate(Owner=na.cover(Owner, Parent)) ->
    berthdata

  read_xlsx('data/GEM-LNG-Terminals-2022-03-16.xlsx') %>%
    select(GEM.LNG.TerminalID=TerminalID, Owner.GEM=Owner) %>%
    left_join(berthdata, .) %>% mutate(Owner=na.cover(Owner, Owner.GEM)) %>%
    select(-Owner.GEM) ->
    berthdata

  berthdata %<>% select(name = Polygon.name, Port.country, Port.UN.LOCODE, Product, Owner, Linked.companies, contains('GEM'))

  berthdata$Owner %>%
    disambiguate(c('Exxon', 'Shell', 'Total', 'Repsol', 'BP', 'Lukoil',
                   'Neste', 'Orlen',
                   'RWE', 'KEPCO', 'Taipower', 'Chubu Electric Power', 'TEPCO', 'Kyushu Electric Power',
                   'Nippon Steel', 'Tohoku Electric Power', 'POSCO', 'Formosa Petrochemical Corporation',
                   'Mitsubishi', 'Hyundai Steel', 'Sumitomo', 'JFE Steel')) ->
    berthdata$Owner_simple

  berthdata$Owner_simple[grep('Pohang', berthdata$Owner)] <- 'POSCO'
  berthdata$Owner_simple[grep('Chubu.*TEPCO', berthdata$Owner)] <- 'Chubu Electric Power, TEPCO'
  berthdata$Owner_simple %<>% gsub("JERA|JERA Global Markets", 'Chubu Electric Power, TEPCO', .)

  ind = grep('Associated Petroleum Terminals', berthdata$Owner)
  berthdata$Owner_simple[ind] <- berthdata$Linked.companies[ind]

  ind = grep('Stakeholders', berthdata$Owner)
  berthdata$Owner_simple[ind] <-
    paste0(berthdata$name, ' (', berthdata$Linked.companies, ')')[ind] %>%
    gsub(',BP', ', BP', .) %>% gsub('\\.)', ')', .)

  berthdata %>% distinct(name, .keep_all=T)
}

addBerthData <- function(df, version='v7') {
  getBerthData(version) -> berthdata

  berthdata %>% filter(Port.country!='Russia', !is.na(Owner)) %>%
    select(name, arrival_berth_owner=Owner, arrival_berth_owner_simple=Owner_simple) %>%
    left_join(df, .)
}




