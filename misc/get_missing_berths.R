require(tidyverse); require(magrittr); require(lubridate)
require(sf)
require(pbapply)
library(httr)
library(jsonlite)
require(countrycode)
library(creahelpers)
library(sp)


positions <- read_csv("https://fossil-shipment-tracker.ew.r.appspot.com/v0/position?speed_max=0.1&has_arrival_berth=False&status=completed&format=csv&buffer_hour=24")

# We keep only positions around arrival time,
# and exclude bulk that is not coal
positions <- positions %>%
  filter(abs(date_utc-arrival_date_utc) < lubridate::hours(24)) %>%
  mutate(is_coal = grepl("coal", departure_berth_commodity, ignore.case = T)) %>%
  filter(is_coal | !grepl("Bulk Carrier", ship_subtype, ignore.case=T))

cluster <- function(sf, distKM) {
  require(geosphere)
  sp <- to_spdf(sf)
  hc <- sp %>% coordinates %>% distm %>% as.dist %>% hclust
  cutree(hc,h=distKM*1000)
}

berths <- positions %>%
  group_by(voyage_id, is_coal, ship_type, ship_subtype, imo) %>%
  group_modify(function(df, ...) {

    df$cluster <- 1
    if(nrow(df)>1){
      df$cluster <- df %>% sf::st_as_sf(coords=c("lon","lat")) %>% cluster(.1)
    }

    df %>%
      group_by(cluster) %>%
      summarise(across(c(lon, lat), mean),
                duration = date_utc %>% (function(x) max(x)-min(x)),
                arrival_to_position = min(date_utc) - min(arrival_date_utc)) %>%
      ungroup %>% select(-cluster)
  })

berths_sf <- berths %>%
  ungroup() %>%
  mutate(duration_hour = paste0(as.numeric(duration, unit="hours"), "hours")) %>%
  mutate_at(c("is_coal", "imo"), as.character) %>%
  sf::st_as_sf(coords=c("lon","lat")) %>%
  st_set_crs(4326)

filepath <- "missing_berths.kml"
if(file.exists(filepath)) file.remove(filepath)
sf::st_write(berths_sf, filepath)

