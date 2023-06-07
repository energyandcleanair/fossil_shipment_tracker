from engine.kpler_scraper.scraper import *

### IMPORTANT
### Certain country names and to_zone_name are still empty after
### scraping, and have been updated manually in the database
### Before scraping again, add a constraint or a check that this is not the case
###
### Other required fixes:
### - Singapore has duplicates: zone_id 1109 and 833
### - Koweit has duplicates:zone_id 110755 and 505
### Ended up removing 833 and 110755 as having the lowest values
class KplerFlowScraper(KplerScraper):
    def get_flows_raw(
        self,
        platform,
        origin_iso2=None,
        destination_iso2=None,
        destination_country=None,
        from_installation=None,
        to_installation=None,
        product=None,
        date_from=None,
        date_to=None,
        split=None,
        granularity=FlowsPeriod.Daily,
        unit=FlowsMeasurementUnit.T,
    ):
        origin_country = (
            unidecode(self.cc.convert(origin_iso2, to="name_short")) if origin_iso2 else None
        )

        if destination_iso2 is not None and destination_country is None:
            destination_country = unidecode(self.cc.convert(destination_iso2, to="name_short"))

        params = {
            "from_zones": [origin_country] if not from_installation else None,
            "to_zones": [destination_country] if destination_country else None,
            "products": product,
            "from_installations": from_installation,
            "to_installations": to_installation,
            "flow_direction": [FlowsDirection.Export],
            "split": [split],
            "granularity": [granularity],
            "start_date": to_datetime(date_from),
            "end_date": to_datetime(date_to),
            "unit": [unit],
            "with_forecast": False,
            "with_intra_country": False,
        }

        try:
            try:
                df = self.flows_clients[platform].get(**params)
            except requests.exceptions.ChunkedEncodingError:
                time.sleep(3)
                df = self.flows_clients[platform].get(**params)
        except exceptions.HttpError as e:
            logger.warning(f"Kpler API error: {e}")
            return None

        if "Date" not in df.columns:
            logger.warning(f"No date in Kpler data: {params} {df}")
            return None

        if "Period End Date" in df.columns:
            df.drop("Period End Date", axis=1, inplace=True)

        df.rename(columns={"Date": "date"}, inplace=True)
        df = df.melt(id_vars=["date"], var_name="split")
        df["unit"] = unit.value
        return df

    def get_trades_raw(
        self,
        platform,
        from_zone=None,
        to_zone=None,
        cursor_after=0,
        product=None,
    ):

        if from_zone and from_zone.get("name") == "Unknown":
            return None

        from_locations = (
            [
                self.get_zone_dict(
                    id=from_zone.get("id"), name=from_zone.get("name"), platform=platform
                )
            ]
            if from_zone
            else []
        )

        from_locations = [x["resourceType"][0].lower() + str(x["id"]) for x in from_locations]
        to_locations = (
            [self.get_zone_dict(id=to_zone.get("id"), name=to_zone.get("name"), platform=platform)]
            if to_zone
            else []
        )
        to_locations = [x["resourceType"][0].lower() + str(x["id"]) for x in to_locations]
        # from: 0
        # size: 100
        # view: kpler
        # withForecasted: false
        # withFreightView: false
        # withProductEstimation: false
        # locations: z757

        # Get zone dict
        params_raw = {
            "from": cursor_after,
            "size": 1000,
            "view": "kpler",
            "withForecasted": "false",
            "withFreightView": "false",
            "withProductEstimation": "false",
            "locations": from_locations,
        }

        if product is not None:
            params_raw["variables"]["where"]["productIds"] = [
                self.get_product_id(platform=platform, name=product)
            ]

        token = self.token  # get_env("KPLER_TOKEN_BRUTE")
        url = {
            "dry": "https://dry.kpler.com/api/trades",
            "liquids": "https://terminal.kpler.com/api/trades",
            "lng": "https://lng.kpler.com/api/trades",
        }.get(platform)

        headers = {
            "Authorization": f"Bearer {token}",
            "x-web-application-version": "v21.316.0",
            "content-type": "application/json",
        }
        try:
            r = requests.get(url, params=params_raw, headers=headers)
        except requests.exceptions.ChunkedEncodingError:
            logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
            return None

        trades_raw = r.json()

        set(x.get("status") for x in trades_raw)

        def parse_trade(x):

            trade = {}

            # General
            trade["id"] = x.get("id")
            status_dict = {
                "In Transit": base.ONGOING,
                "Delivered": base.COMPLETED,
            }
            if x.get("status") not in status_dict:
                return None
            else:
                trade["status"] = status_dict[x.get("status")]

            trade["departure_date_utc"] = pd.to_datetime(x.get("start"))
            trade["arrival_date_utc"] = pd.to_datetime(x.get("end"))

            # Berth
            trade["departure_berth_name"] = x.get("berth", {}).get("name")
            trade["departure_berth_id"] = x.get("berth", {}).get("id")

            # Flows
            flows = x.get("flowQuantities")
            if len(flows) > 1:
                logger.warning(f"More than one flow in trade {x.get('id')}. Not managed yet.")
                return None
            elif len(flows) == 0:
                return None

            flow = flows[0]
            trade["commodity_name"] = (
                flow.get("confirmedProduct").get("closestAncestorCommodity", {}).get("name")
            )

            trade["commodity_id"] = (
                flow.get("confirmedProduct").get("closestAncestorCommodity", {}).get("id")
            )

            trade["product_id"] = flow.get("confirmedProduct").get("productId")
            trade["product_name"] = flow.get("confirmedProduct").get("name")
            trade["product_type"] = flow.get("confirmedProduct").get("type")

            return trades, installations, berths, vessels

        trades = [parse_trade(x) for x in trades_raw]
        try:
            cursor, voyages_data = response_data["cursors"]["after"], response_data["items"]
        except KeyError:
            logger.warning("Missing data. Returning")
            return None

        voyages_infos = []
        for voyage in voyages_data:

            def parse_portcalls(portcalls):
                """
                Read the portcalls of a single voyage, and return key voyage information
                i.e. from_installation, to_installation, quantity, unit, product_id, status
                :param portcalls:
                :return:
                """
                if not portcalls:
                    raise ValueError("No portcalls found")

                load_portcalls = [x for x in portcalls if x["operation"] == "LOAD"]
                discharge_portcalls = [x for x in portcalls if x["operation"] == "DISCHARGE"]

                status = base.UNKNOWN
                try:
                    departure_zone_id = load_portcalls[0]["zone"]["id"]
                    departure_zone_name = load_portcalls[0]["zone"]["name"]
                except IndexError:
                    departure_zone_id = None
                    departure_zone_name = None

                if load_portcalls and load_portcalls[0].get("installation"):
                    departure_installation_id = load_portcalls[0]["installation"]["id"]
                    departure_installation_name = load_portcalls[0]["installation"]["name"]
                else:
                    departure_installation_id = None
                    departure_installation_name = None

                if not discharge_portcalls:
                    status = base.ONGOING
                    arrival_zone_id = None
                    arrival_zone_name = None
                    arrival_installation_id = None
                    arrival_installation_name = None
                elif discharge_portcalls:
                    status = base.COMPLETED

                    try:
                        arrival_zone_id = discharge_portcalls[-1]["zone"]["id"]
                        arrival_zone_name = discharge_portcalls[-1]["zone"]["name"]
                    except IndexError:
                        arrival_zone_id = None
                        arrival_zone_name = None

                    if discharge_portcalls[-1].get("installation"):
                        arrival_installation_id = discharge_portcalls[-1]["installation"]["id"]
                        arrival_installation_name = discharge_portcalls[-1]["installation"]["name"]
                    else:
                        arrival_installation_id = None
                        arrival_installation_name = None

                # Get quantities info
                last_portcall = portcalls[-1]
                flows = last_portcall["flowQuantities"]
                if not flows:
                    # Even if not flow, let's store the shipment
                    products = [{}]
                    quantities = [{}]
                else:
                    products = [x["product"] for x in flows]
                    quantities = [x["flowQuantity"] for x in flows]

                def abs_or_none(x):
                    if x is None:
                        return None
                    return abs(x)

                result = [
                    {
                        "departure_zone_id": departure_zone_id,
                        "departure_zone_name": departure_zone_name,
                        "departure_installation_id": departure_installation_id,
                        "departure_installation_name": departure_installation_name,
                        "arrival_zone_id": arrival_zone_id,
                        "arrival_zone_name": arrival_zone_name,
                        "arrival_installation_id": arrival_installation_id,
                        "arrival_installation_name": arrival_installation_name,
                        "status": status,
                        "product_id": products[i].get("id"),
                        "product_name": products[i].get("name"),
                        "value_tonne": abs_or_none(quantities[i].get("mass")),
                        "value_m3": abs_or_none(quantities[i].get("volume")),
                    }
                    for i in range(len(products))
                ]
                return result

            # forcing check of portcall data to make sure we dont mess up here
            vessels = self.get_vessels_brute(platform=platform)
            vessel_id = voyage.get("vessel")["id"]
            try:
                vessel_imo = vessels.imo[vessels.id.astype(int) == int(vessel_id)].values[0]
            except IndexError:
                vessel_imo = None

            voyage_infos = {
                "id": voyage.get("id"),
                "departure_date": dt.datetime.strptime(
                    voyage.get("start"), "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                "arrival_date": dt.datetime.strptime(voyage.get("end"), "%Y-%m-%dT%H:%M:%S.%fZ"),
                "vessel_id": vessel_id,
                "vessel_imo": str(vessel_imo),  # redundant, but just in case
                "others": {"kpler": voyage},
            }

            portcall_infos = parse_portcalls(voyage.get("portCalls"))

            # One voyage can have several flows
            def update_and_return(x, y):
                x_copy = x.copy()
                x_copy.update(y)
                return x_copy

            voyage_infos = [update_and_return(x, voyage_infos) for x in portcall_infos]
            voyages_infos.extend(voyage_infos)

        voyages_df = pd.DataFrame(voyages_infos)
        return cursor, voyages_df

    def get_flows_raw_brute(
        self,
        platform,
        date_from,
        date_to,
        split,
        from_zone=None,
        to_zone=None,
        product=None,
        unit=None,
        granularity=FlowsPeriod.Daily,
        include_total=True,
    ):
        """
        This one uses the token from the web interface,
        and another payload, that allows us to go back further than 1 year
        :param params:
        :param platform:
        :return:
        """
        if from_zone and from_zone.get("name") == "Unknown":
            return None

        from_locations = (
            [
                self.get_zone_dict(
                    id=from_zone.get("id"), name=from_zone.get("name"), platform=platform
                )
            ]
            if from_zone
            else []
        )

        to_locations = (
            [self.get_zone_dict(id=to_zone.get("id"), name=to_zone.get("name"), platform=platform)]
            if to_zone
            else []
        )

        params_raw = {
            "cumulative": False,
            # "filters": {"product": [1334]},
            "filters": {"product": []},
            "flowDirection": "export",
            # "fromLocations": [{"id": 451, "resourceType": "zone"}],
            "fromLocations": from_locations,
            "toLocations": to_locations,
            "granularity": granularity.value,
            "interIntra": "interintra",
            "onlyRealized": True,
            "view": "kpler",
            "withBetaVessels": False,
            "withForecasted": False,
            "withGrades": False,
            "withIncompleteTrades": True,
            "withIntraCountry": False,
            "vesselClassifications": [],
            "withFreightView": False,
            "withProductEstimation": False,
            "splitOn": split.value,
            "startDate": to_datetime(date_from).strftime("%Y-%m-%d"),
            "endDate": to_datetime(date_to).strftime("%Y-%m-%d"),
        }

        if to_zone is not None:
            params_raw["toLocations"] = [
                self.get_zone_dict(
                    id=to_zone.get("id"), name=to_zone.get("name"), platform=platform
                )
            ]

        if product is not None:
            if isinstance(product, dict):
                params_raw["filters"] = {"product": [int(product.get("id"))]}
            else:
                params_raw["filters"] = {
                    "product": [self.get_product_id(platform=platform, name=product)]
                }
        else:
            default_products = {"liquids": [1400, 1328, 1370], "lng": [1750], "dry": [1334]}
            params_raw["filters"] = {"product": default_products[platform]}

        token = self.token  # get_env("KPLER_TOKEN_BRUTE")
        url = {
            "dry": "https://dry.kpler.com/api/flows",
            "liquids": "https://terminal.kpler.com/api/flows",
            "lng": "https://lng.kpler.com/api/flows",
        }.get(platform)
        headers = {
            "Authorization": f"Bearer {token}",
            "x-web-application-version": "v21.316.0",
            "content-type": "application/json",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) ",
        }
        try:
            r = self.session.post(url, json=params_raw, headers=headers)
        except (requests.exceptions.ChunkedEncodingError, urllib3.exceptions.ReadTimeoutError):
            logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
            return None

        # read content to dataframe
        try:
            data = r.json()["series"]
        except requests.exceptions.JSONDecodeError:
            logger.warning(f"Kpler request failed: {params_raw}. Probably empty")
            return None

        dfs = []
        for x in data:
            df = pd.concat(
                [pd.DataFrame(y["splitValues"]) for y in x["datasets"]], ignore_index=True
            )
            if len(df) > 0:
                df = pd.concat([df.drop(["values"], axis=1), df["values"].apply(pd.Series)], axis=1)
                df["date"] = x["date"]
                dfs += [df]

            # Add total
            # if include_total:
            #     df_total = pd.DataFrame([y["values"] for y in x["datasets"]])
            #     df_total["date"] = x["date"]
            #     df_total["id"] = KPLER_TOTAL
            #     df_total["name"] = KPLER_TOTAL
            #     dfs += [df_total]

        if not dfs:
            return None

        df = pd.concat(dfs, ignore_index=True)
        df.rename(columns={"name": "split_name", "id": "split_id"}, inplace=True)
        df["split"] = df.apply(
            lambda row: {"id": row["split_id"], "name": row["split_name"]}, axis=1
        )
        df.drop(["split_id", "split_name"], axis=1, inplace=True)
        df = df.melt(id_vars=["date", "split"])
        df["date"] = pd.to_datetime(df["date"])

        units = {
            "mass": FlowsMeasurementUnit.T.value,
            "volume": "m3",
            "energy": "GJ?",
        }
        # Recode variable to unit using the dictionary
        df["unit"] = df.variable.map(units)
        df = df[~pd.isna(df.unit)]
        if unit:
            df = df[df.unit == unit.value]
        df.drop(["variable"], axis=1, inplace=True)
        return df

    def get_flows(
        self,
        platform,
        origin_iso2=None,
        destination_iso2=None,
        product=None,
        split=None,
        from_zone=None,
        from_split=FlowsSplit.OriginCountries,
        to_zone=None,
        to_split=FlowsSplit.DestinationCountries,
        granularity=FlowsPeriod.Daily,
        unit=FlowsMeasurementUnit.T,
        date_from=dt.datetime.now() - dt.timedelta(days=365),
        date_to=dt.datetime.now(),
        use_brute_force=True,
    ):

        if from_zone is None and origin_iso2 is not None:
            if from_split == FlowsSplit.OriginCountries:
                from_zone = self.get_zone_dict(platform=platform, iso2=origin_iso2)
            else:
                raise ValueError("Wrong from_zone indication")

        if to_zone is None and destination_iso2 is not None:
            if to_split == FlowsSplit.DestinationCountries:
                to_zone = self.get_zone_dict(platform=platform, iso2=destination_iso2)
            else:
                raise ValueError("Wrong to_zone indication")

        params = {
            "from_zone": from_zone,
            "to_zone": to_zone,
            "product": product,
            "split": split,
            "granularity": granularity,
            "unit": unit,
            "date_from": date_from,
            "date_to": date_to or dt.datetime.now(),
        }

        if use_brute_force:
            df = self.get_flows_raw_brute(platform=platform, **params, include_total=False)
        else:
            df = self.get_flows_raw(platform=platform, **params)
        if df is None:
            return None

        # if destination_iso2 is None and destination_country is not None:
        #     destination_iso2 = self.cc.convert(destination_country, to="ISO2")
        #
        # if destination_iso2 is not None and destination_country is None:
        #     destination_country = self.cc.convert(destination_iso2, to="name_short")

        # Ideally no NULL otherwise the unique constraints won't work
        # This should work from Postgres 15 onwards
        df["from_split"] = get_split_name(from_split)
        df["to_split"] = get_split_name(to_split)
        df["from_iso2"] = origin_iso2 if origin_iso2 else KPLER_TOTAL

        if product is None:
            product_name = KPLER_TOTAL
        elif isinstance(product, str):
            product_name = product
        elif isinstance(product, dict):
            product_name = product.get("name")
        else:
            raise ValueError(f"Unknown product type: {type(product)}")

        df["from_zone"] = df.apply(lambda x: from_zone or {"id": 0, "name": None}, axis=1)
        df["to_zone"] = df.apply(lambda x: to_zone or {"id": 0, "name": None}, axis=1)
        df["product"] = product_name
        df["unit"] = unit.value
        df["platform"] = platform
        df = df.rename(columns={"Date": "date"})

        def split_to_column(df, split):
            if split in [
                FlowsSplit.DestinationCountries,
                FlowsSplit.DestinationInstallations,
                FlowsSplit.DestinationPorts,
            ]:
                df["to_zone"] = df["split"]
            elif split in [
                FlowsSplit.OriginCountries,
                FlowsSplit.OriginInstallations,
                FlowsSplit.OriginPorts,
            ]:
                df["from_zone"] = df["split"]
            elif split == FlowsSplit.Products:
                df["product"] = df["split"].apply(lambda x: x.get("name"))

            return df

        df = split_to_column(df, split)
        df = df.drop(columns=["split"])

        df["from_zone_id"] = df.from_zone.apply(lambda x: self.fix_zone_id(int(x.get("id"))))
        df["to_zone_id"] = df.to_zone.apply(lambda x: self.fix_zone_id(int(x.get("id"))))

        df["to_iso2"] = df.to_zone_id.apply(lambda x: self.get_zone_iso2(platform=platform, id=x))
        df["from_iso2"] = df.from_zone_id.apply(
            lambda x: self.get_zone_iso2(platform=platform, id=x)
        )

        df["from_zone_name"] = df.from_zone.apply(
            lambda x: self.get_zone_name(platform=platform, id=x.get("id"), name=x.get("name"))
        )
        df["to_zone_name"] = df.to_zone.apply(
            lambda x: self.get_zone_name(platform=platform, id=x.get("id"), name=x.get("name"))
        )

        df.drop(columns=["from_zone", "to_zone"], inplace=True)
        return df

    def get_products(self, platform=None):
        platforms = self.platforms if platform is None else [platform]

        def get_platform_products(platform):
            if self.products.get(platform) is None:
                # This yields 17 commodities while we had 20 when using the API
                # products = self.get_products_brute(platform=platform)
                # products = products[~pd.isna(products.closestAncestorCommodity)]
                # products = products[~pd.isna(products.closestAncestorGroup)]
                # commodities = products.closestAncestorCommodity.apply(lambda x: pd.Series(x))
                # commodities["group_name"] = products.closestAncestorGroup.apply(lambda x: x.get('name'))
                # commodities["family_name"] = products.closestAncestorFamily.apply(lambda x: x.get('name'))
                # commodities["belongs_to_platform"] = products.ancestors.apply(lambda x: any([y.get('name').lower() == platform and y.get('type') == 'family' for y in x]))
                # commodities = commodities[commodities.belongs_to_platform]
                # commodities = commodities.drop_duplicates()
                # commodities["platform"] = platform
                # commodities.rename(
                #     columns={
                #         "family_name": "family",
                #         "group_name": "group",
                #     },
                #     inplace=True,
                # )
                # columns = ["id", "name", "type", "family", "group"]
                # self.products[platform] = commodities[columns]
                products = pd.read_sql(
                    KplerProduct.query.filter(KplerProduct.platform == platform).statement,
                    session.bind,
                )
                self.products[platform] = products

            return self.products.get(platform)

        df = pd.concat([get_platform_products(platform) for platform in platforms])
        # df = df[["id", "name", "family", "type", "group", "platform"]].drop_duplicates()
        df = df[["name", "family", "group", "platform"]].drop_duplicates()
        df = df[~pd.isna(df.name)]
        return df

    def get_product_id(self, platform, name):
        manual_values = {"Crude/Co": 1370}
        if name in manual_values:
            return manual_values[name]

        products = self.get_products(platform=platform)
        product = products[products.name == name]
        return int(product.id.values[0]) if len(product) == 1 else None

    def fix_zone_id(self, id):
        """
        Certain zone ids are different based on whether we query with
        specified destination or specified origin. Meaning there can be double counting.
        We clean the zones in this function.
        MUST be run after the flows have been scraped!
        :return:
        """
        manual_fixes = {"1109": "833"}  # SINGAPORE vs SINGAPORE REPUBLIC
        return type(id)(manual_fixes.get(id, id))


def fill_products():
    scraper = KplerScraper()
    products = scraper.get_products()
    upsert(products, DB_TABLE_KPLER_PRODUCT, "kpler_product_pkey")
    return
