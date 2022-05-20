require(tidyverse); require(magrittr); require(lubridate)
require(sf)
require(pbapply)
library(httr)
library(jsonlite)
require(countrycode)
library(creahelpers)
library(sp)


buffer_hour <- 12
speed_max_knot <- 0.1

# Collect missing positions around arrivals
arrival_positions <-
  read_csv(sprintf("https://api.russiafossiltracker.com/v0/position?speed_max=%f&has_arrival_berth=False&around_arrival_only=True&status=completed&format=csv&buffer_hour=%d", speed_max_knot, buffer_hour))

# Collect missing positions around departures
departure_positions <-
  read_csv(sprintf("https://api.russiafossiltracker.com/v0/position?speed_max=%f&has_departure_berth=False&around_departure_only=True&status=completed&format=csv&buffer_hour=%d", speed_max_knot, buffer_hour))


ships <-  read_csv("https://api.russiafossiltracker.com/v0/ship?format=csv")

# voy <- read_csv("https://api.russiafossiltracker.com/v0/voyage?format=csv&nest_in_data=False")
#
# voy %<>% select(voyage_id=id, destination_iso2, destination_country, arrival_port_name)
#
# # We keep only positions around arrival time,
# # and exclude bulk that is not coal
# arrival_positions %<>%
#   filter(date_utc-arrival_date_utc < lubridate::hours(72),
#          date_utc-arrival_date_utc > -lubridate::hours(2)) %>%
#   mutate(is_coal = grepl("coal", departure_berth_commodity, ignore.case = T)) %>%
#   left_join(voy) %>%
#   filter(is_coal | !grepl("Bulk Carrier", ship_subtype, ignore.case=T),
#          destination_iso2 %in% codelist$iso2c[!is.na(codelist$eu28)] |
#            grepl('US|TR|GI|JP|NO|MT|GB|IL|TW|CA', destination_iso2),
#          departure_date_utc>='2022-02-24')
#
# departure_positions <- departure_positions %>%
#   left_join(voy) %>%
#   filter(date_utc-departure_date_utc < lubridate::hours(2),
#          date_utc-departure_date_utc > -lubridate::hours(72),
#          ship_subtype=='Bulk Carrier',
#          destination_iso2 != 'RU')

cluster <- function(sf, distKM) {
  require(geosphere)
  sp <- to_spdf(sf, llcols=c('lon', 'lat'))
  hc <- sp %>% coordinates %>% distm %>% as.dist %>% hclust
  cutree(hc,h=distKM*1000)
}

berths <- bind_rows(arrival_positions %>%
                      mutate(direction='arrival'),
                    departure_positions %>% mutate(direction='departure')) %>%
  mutate(imo=as.character(imo)) %>%
  left_join(ships %>% select(imo, ship_type=type, ship_subtype=subtype)) %>%
  group_by(voyage_id, ship_type, ship_subtype, imo, direction) %>%
  group_modify(function(df, ...) {

    df$cluster <- 1
    if(nrow(df)>1){
      df$cluster <- df %>% sf::st_as_sf(coords=c("lon","lat")) %>% cluster(.5)
    }

    df %>%
      group_by(cluster) %>%
      summarise(across(c(lon, lat), mean),
                duration = date_utc %>% (function(x) max(x)-min(x)),
                arrival_to_position = min(date_utc)) %>%
      ungroup %>% select(-cluster)
  })

berths <- berths %>%
  filter(duration>hours(6)) %>%
  ungroup() %>%
  mutate(duration_hour = paste0(as.numeric(duration, unit="hours"), "hours"),
         Name = paste(ship_subtype, imo)) %>%
  mutate_at(c("imo"), as.character)

berths %>% filter(direction=='departure') %>% write_csv('bulk_ships_missing_departure_berths.csv')
berths %>% filter(direction=='arrival') %>% write_csv('missing_arrival_berths.csv')

berths_sf <- berths %>%
  sf::st_as_sf(coords=c("lon","lat")) %>%
  st_set_crs(4326)

filepath <- "missing_berths.kml"
if(file.exists(filepath)) file.remove(filepath)
sf::st_write(berths_sf, filepath)

