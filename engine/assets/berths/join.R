library(readxl)
library(tidyverse)
library(sf)
library(magrittr)

require(tidyverse); require(magrittr); require(lubridate)
require(pbapply)
require(countrycode)
require(readxl)
library(rcrea)

source('add_berth_owners.R')

# data <- readxl::read_xlsx('berth_infos.xlsx',
#                   .name_repair = make.names) %>%
#   filter(!is.na(Polygon.name))

data <- getBerthData()

berths <- st_read('berths.kml')

fixnames <- function(x) {
  x %>% stringi::stri_trans_general("Latin-ASCII") %>%
    gsub(' $', '', .) %>% gsub('Refinery\\.', 'Refinery', .) %>% gsub('Termials', 'Terminals', .)
}

berths$Name %<>% fixnames
# data$Polygon.name %<>% fixnames
berths_joined <- berths %>%
  rename(name=Name) %>%
  left_join(data)

berths_joined %>%
  mutate(port_unlocode = gsub(" ","", Port.UN.LOCODE),
         commodity=Product,
         id=seq(1, nrow(berths_joined))) %>%
  select(id, name, port_unlocode, commodity, owner=Owner, geometry) %>%
  sf::write_sf("berths_joined.geojson")
