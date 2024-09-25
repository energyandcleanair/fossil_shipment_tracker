from typing import Optional
from bs4 import BeautifulSoup
import datetime as dt
import pandas as pd
import re
import base
from base.logger import logger

from engines.company_scraper.session_management import (
    EquasisSessionManager,
    OnDemandEquasisSessionManager,
)


class EquasisClient:
    def __init__(self, *, session_manager: Optional[EquasisSessionManager] = None):
        if session_manager is None:
            self.session_manager = OnDemandEquasisSessionManager.with_account_generator()
        else:
            self.session_manager = session_manager

    def _clean_text(self, text):
        text = text.replace("\t", "").replace("\r", "").replace("\n", "")
        return text

    def _find_pnis(self, parent):
        pni_divs = parent.find_all("div", attrs={"class": "access-body"})
        if not len(pni_divs) > 0:
            return []

        results = []

        for pni_div in pni_divs:
            pni_ps = pni_div.find_all("p")
            if not len(pni_ps) > 1:
                break

            result = {}
            result["name"] = self._clean_text(pni_ps[0].text)

            def extract_inception_date(p):
                if p.startswith("Inception at "):
                    # Extract date from date_from_p that can be in various formats, including dd/mm/YYY, YYYY-mm-dd
                    formats = ["%d/%m/%Y", "%Y-%m-%d"]
                    for format in formats:
                        try:
                            result = dt.datetime.strptime(p.replace("Inception at ", ""), format)
                            return result.date()
                        except ValueError:
                            continue
                return None

            date_from = extract_inception_date(self._clean_text(pni_ps[1].text))
            if date_from:
                result["date_from"] = date_from
            results.append(result)
        return results

    def _find_management(self, parent):
        resp = []
        form = parent.find("form", attrs={"name": "formShipToComp"})
        if not form:
            return resp

        trs = form.find_all("tr")
        if not trs or not len(trs) > 1:
            return resp

        trs = trs[1:]

        for tr in trs:
            try:
                data = {}
                tds = tr.find_all("td")
                data["imo"] = self._clean_text(tds[0].text)
                data["role"] = self._clean_text(tds[1].text)
                data["company"] = self._clean_text(tds[2].text)
                data["address"] = self._clean_text(tds[3].text)
                data["doa"] = self._clean_text(tds[4].text)
                resp.append(data)
            except IndexError:
                continue

        return resp

    def _log(self, message):
        print(message)

    def _parse_doa(self, doa):
        formats = ["since %d/%m/%Y", "during %m/%Y", "before %m/%Y"]
        for format in formats:
            try:
                return dt.datetime.strptime(doa, format)
            except ValueError:
                continue
        return None

    def get_ship_infos(self, imo, itry=1, max_try=11):
        logger.info(f"Getting ship info for IMO {imo}")
        if itry > max_try:
            return None

        url = "https://www.equasis.org/EquasisWeb/restricted/ShipInfo?fs=Search"
        ship_data = {}
        ship_data["imo"] = imo
        ship_data["updated_on"] = dt.datetime.now()
        payload = {"P_IMO": imo}

        resp = self.session_manager.make_request(url, payload)

        html_obj = BeautifulSoup(resp, "html.parser")

        if not html_obj or not html_obj.body:
            return None

        # Insurer
        pni_div = html_obj.body.find("div", attrs={"id": "collapse6"})
        if pni_div:
            ship_data["insurers"] = self._find_pnis(pni_div)

        # Manager & Owner
        management_div = html_obj.body.find("div", attrs={"id": "collapse3"})
        if management_div:
            management_raw = self._find_management(management_div)
            try:
                manager_info = next(x for x in management_raw if x["role"] == "ISM Manager")
                ship_data["manager"] = {
                    "name": manager_info.get("company"),
                    "imo": manager_info.get("imo"),  # IMO of company
                    "address": manager_info.get("address"),
                    "date_from": self._parse_doa(manager_info.get("doa")),
                }
            except StopIteration:
                pass

            try:
                owner_info = next(x for x in management_raw if x["role"] == "Registered owner")
                ship_data["owner"] = {
                    "name": owner_info.get("company"),
                    "imo": owner_info.get("imo"),  # IMO of company
                    "address": owner_info.get("address"),
                    "date_from": self._parse_doa(owner_info.get("doa")),
                }
            except StopIteration:
                pass

        if list(ship_data.keys()) == ["imo"]:
            pass

        # If (and only if) we have owner or manager but no insurer, we'll add an empty insurer
        if "owner" in ship_data or "manager" in ship_data:
            if "insurers" not in ship_data or len(ship_data["insurers"]) == 0:
                logger.debug("No owner, manager or insurer found for ship %s:" % (imo))
                ship_data["insurers"] = [{"name": base.UNKNOWN_INSURER}]

        ship_data["current_flag"] = self._extract_flag(html_obj)

        return ship_data

    def _extract_flag(self, html_obj):
        flag_label_div = html_obj.body.find("b", string=re.compile("Flag"))
        if not flag_label_div:
            return None
        flag_wrapper_div = flag_label_div.parent.parent
        flag_divs = flag_wrapper_div.find_all("div")
        if not flag_divs:
            return None
        flag_text = flag_divs[-1].get_text()
        if not flag_text:
            return None
        return self._clean_text(flag_text.replace("(", "").replace(")", ""))

    def get_inspections(self, imo, itry=1, max_try=11):
        if itry > max_try:
            return None

        url = "https://www.equasis.org/EquasisWeb/restricted/ShipInspection?fs=ShipInfo"
        ship_data = {}
        ship_data["imo"] = imo
        payload = {"P_IMO": imo}

        resp = self.session_manager.make_request(url, payload)

        html_obj = BeautifulSoup(resp, "html.parser")

        if not html_obj or not html_obj.body:
            return None

        # Find the table containing the inspections
        table = html_obj.find("table", {"class": "tableLSDD"})

        if not table:
            return None

        # Extract the table headers
        headers = [header.text.strip() for header in table.find_all("th")]

        assert headers == [
            "Authority",
            "Port of inspection",
            "Date of report",
            "Detention",
            "PSC Organisation",
            "Type of inspection",
            "Duration (days)",
            "Number of deficiencies",
            "Details",
        ]

        # Extract the table rows
        rows_data = []
        shared_columns_length = 4
        row_elements = table.find("tbody").find_all("tr", recursive=False)
        for row in row_elements:
            cells = row.find_all(["td", "th"], recursive=False)
            contents = [cell.text.strip() for cell in cells]
            rows_data.append(contents)

            child_rows = row.find_all("tr", recursive=False)
            for child_row in child_rows:
                child_cells = child_row.find_all(["td", "th"], recursive=False)
                child_contents = contents[:shared_columns_length] + [
                    child_cell.text.strip() for child_cell in child_cells
                ]
                rows_data.append(child_contents)

        # Create a DataFrame from the extracted data
        df = pd.DataFrame(rows_data, columns=headers).drop(columns=["Details"])
        df.replace("", pd.NA, inplace=True)
        # Convert number of deficiencies to number
        df["Number of deficiencies"] = pd.to_numeric(df["Number of deficiencies"], errors="coerce")
        df["Duration (days)"] = pd.to_numeric(df["Duration (days)"], errors="coerce")

        ship_data["inspections"] = df

        date_formats = ["%d/%m/%Y", "%Y-%m-%d"]

        def _parse_date(date_str):
            for date_format in date_formats:
                try:
                    return dt.datetime.strptime(date_str, date_format).date()
                except ValueError:
                    continue
            raise ValueError(f"Could not parse date {date_str} with formats {date_formats}")

        ship_data["inspections"]["Date of report"] = ship_data["inspections"][
            "Date of report"
        ].apply(_parse_date)
        ship_data["inspections"]["Date of report"] = pd.to_datetime(
            ship_data["inspections"]["Date of report"]
        )

        return ship_data

    def get_ship_history(self, imo, itry=1, max_try=11):
        if itry > max_try:
            return None

        url = "https://www.equasis.org/EquasisWeb/restricted/ShipHistory?fs=Search"
        ship_data = {}
        ship_data["imo"] = imo
        payload = {"P_IMO": imo}

        resp = self.session_manager.make_request(url, payload)

        html_obj = BeautifulSoup(resp, "html.parser")

        if not html_obj or not html_obj.body:
            return None

        # Table
        company_h3 = html_obj.body.find(
            "h3", text=lambda x: x.startswith("Company") if x else False
        )
        if company_h3:
            table = company_h3.find_parent("div", attrs={"class": "container-fluid"}).find("table")
            # Extract the column headers from the table
            column_headers = [th.text.strip() for th in table.find_all("th")]

            # Extract the data for each row in the table
            data_rows = []
            for tr in table.find_all("tr"):
                data_cells = []
                for td in tr.find_all("td"):
                    data_cells.append(td.text.strip())
                data_rows.append(data_cells)

            # Create a Pandas dataframe with the extracted data
            df = pd.DataFrame(data_rows, columns=column_headers)

        # Manager & Owner
        management_div = html_obj.body.find("div", attrs={"id": "collapse3"})
        if management_div:
            management_raw = self._find_management(management_div)
            try:
                manager_info = next(x for x in management_raw if x["role"] == "ISM Manager")
                ship_data["manager"] = {
                    "name": manager_info.get("company"),
                    "imo": manager_info.get("imo"),  # IMO of company
                    "address": manager_info.get("address"),
                    "date_from": self._parse_doa(manager_info.get("doa")),
                }
            except StopIteration:
                pass

            try:
                owner_info = next(x for x in management_raw if x["role"] == "Registered owner")
                ship_data["owner"] = {
                    "name": owner_info.get("company"),
                    "imo": owner_info.get("imo"),  # IMO of company
                    "address": owner_info.get("address"),
                    "date_from": self._parse_doa(owner_info.get("doa")),
                }
            except StopIteration:
                pass

        if list(ship_data.keys()) == ["imo"]:
            pass

        return ship_data
