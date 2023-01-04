import requests
from bs4 import BeautifulSoup
import datetime as dt
import pandas as pd

from base.env import get_env
from base.utils import to_list


class Equasis():

    session = None
    current_credentials_idx = -1

    def __init__(self):
        self.session = requests.Session()
        self._login()
    
    def _login (self):
        url = 'https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage'
        headers = {'User-Agent': 'Mozilla/5.0'}
        credentials = self._get_next_crendentials()
        payload = {'j_email':credentials['username'],
                   'j_password':credentials['password'],
                   'submit':'Login'}
        try:
            resp = self.session.post(url,headers=headers,data=payload)
        except Exception as e:
            self._log('Error logging in to equasis.org')
            raise e

    def _get_next_crendentials(self):
        credentials = self._get_all_credentials()
        self.current_credentials_idx += 1
        self.current_credentials_idx %= len(credentials)
        next_credentials = credentials[self.current_credentials_idx]
        self._log("Trying with email %s"%(next_credentials['username']))
        return next_credentials

    def _get_all_credentials(self):
        emails = ['hubert+%03d@energyandcleanair.org'%(x) for x in range(1, 22)]
        password = get_env('EQUASIS_PASSWORD')
        return [{'username':x, 'password': password} for x in emails]

    def _clean_text(self, text):
        text = text.replace('\t','').replace('\r','').replace('\n','')
        return text

    def _find_pni(self, parent):
        pni_div = parent.find('div', attrs={'class':'access-body'})
        if not pni_div:
            return
        pni_ps = pni_div.find_all('p')
        if not len(pni_ps)>1:
            return 
        return self._clean_text(pni_ps[0].text)

    def _find_management(self, parent):
        resp = []
        form = parent.find('form', attrs={'name':"formShipToComp"})
        if not form:
            return resp

        trs = form.find_all('tr')
        if not trs or not len(trs)>1:
            return resp

        trs = trs[1:]

        for tr in trs:
            try:
                data = {}
                tds = tr.find_all('td')
                data['imo'] = self._clean_text(tds[0].text)
                data['role'] = self._clean_text(tds[1].text)
                data['company'] = self._clean_text(tds[2].text)
                data['address'] = self._clean_text(tds[3].text)
                data['doa'] = self._clean_text(tds[4].text)
                resp.append(data)
            except IndexError:
                continue

        return resp

    def _log(self, message):
        print(message)

    def _parse_doa(self, doa):
        formats = ['since %d/%m/%Y', 'during %m/%Y', 'before %m/%Y']
        for format in formats:
            try:
                return dt.datetime.strptime(doa, format)
            except ValueError:
                continue
        return None

    def get_ship_infos(self, imo, itry=1, max_try=11):
        
        if itry > max_try:
            return None
        
        url = "https://www.equasis.org/EquasisWeb/restricted/ShipInfo?fs=Search"
        headers = {'User-Agent': 'Mozilla/5.0'}
        ship_data = {}
        ship_data['imo'] = imo
        payload = {
            "P_IMO": imo
        }

        try:
            resp = self.session.post(url,headers=headers,data=payload)
        except Exception as e:
            self._log("Error getting response")
            raise e
        except requests.exceptions.HTTPError as e:
            self._log("HTTP error")
            raise e

        if "session has expired" in str(resp.content):
            self._login()
            return self.get_ship_infos(imo=imo, itry=itry + 1)
        html_obj = BeautifulSoup(resp.content, "html.parser")

        #In case ship info is required
        #info_box = html_obj.body.find('div', attrs={'class':'info-details'})
        if (not html_obj or not html_obj.body) and itry == 1:
            self._login()
            return self.get_ship_infos(imo=imo, itry=itry + 1)

        if not html_obj or not html_obj.body:
            return None

        # Insurer
        pni_div = html_obj.body.find('div', attrs={'id': 'collapse6'})
        if pni_div:
            ship_data['insurer'] = {'name': self._find_pni(pni_div)}

        # Manager & Owner
        management_div = html_obj.body.find('div', attrs={'id': 'collapse3'})
        if management_div:
            management_raw = self._find_management(management_div)
            try:
                manager_info = next(x for x in management_raw if x['role'] == 'ISM Manager')
                ship_data['manager'] = {'name': manager_info.get('company'),
                                        'imo': manager_info.get('imo'), # IMO of company
                                        'address': manager_info.get('address'),
                                        'date_from': self._parse_doa(manager_info.get('doa'))
                                        }
            except StopIteration:
                pass

            try:
                owner_info = next(x for x in management_raw if x['role'] == 'Registered owner')
                ship_data['owner'] = {'name': owner_info.get('company'),
                                      'imo': owner_info.get('imo'),  # IMO of company
                                      'address': owner_info.get('address'),
                                      'date_from': self._parse_doa(owner_info.get('doa')),
                                    }
            except StopIteration:
                pass

        if list(ship_data.keys()) == ['imo']:
            pass

        return ship_data

    def get_ship_history(self, imo, itry=1, max_try=11):

        if itry > max_try:
            return None

        url = "https://www.equasis.org/EquasisWeb/restricted/ShipHistory?fs=Search"
        headers = {'User-Agent': 'Mozilla/5.0'}
        ship_data = {}
        ship_data['imo'] = imo
        payload = {
            "P_IMO": imo
        }

        try:
            resp = self.session.post(url, headers=headers, data=payload)
        except Exception as e:
            self._log("Error getting response")
            raise e
        except requests.exceptions.HTTPError as e:
            self._log("HTTP error")
            raise e

        if "session has expired" in str(resp.content):
            self._login()
            return self.get_ship_infos(imo=imo, itry=itry + 1)
        html_obj = BeautifulSoup(resp.content, "html.parser")

        # In case ship info is required
        # info_box = html_obj.body.find('div', attrs={'class':'info-details'})
        if (not html_obj or not html_obj.body) and itry == 1:
            self._login()
            return self.get_ship_infos(imo=imo, itry=itry + 1)

        if not html_obj or not html_obj.body:
            return None

        # Table
        company_h3 = html_obj.body.find('h3', text=lambda x: x.startswith('Company') if x else False)
        if company_h3:
            table = company_h3.find_parent('div', attrs={'class':'container-fluid'}).find('table')
            # Extract the column headers from the table
            column_headers = [th.text.strip() for th in table.find_all('th')]

            # Extract the data for each row in the table
            data_rows = []
            for tr in table.find_all('tr'):
                data_cells = []
                for td in tr.find_all('td'):
                    data_cells.append(td.text.strip())
                data_rows.append(data_cells)

            # Create a Pandas dataframe with the extracted data
            df = pd.DataFrame(data_rows, columns=column_headers)


        # Manager & Owner
        management_div = html_obj.body.find('div', attrs={'id': 'collapse3'})
        if management_div:
            management_raw = self._find_management(management_div)
            try:
                manager_info = next(x for x in management_raw if x['role'] == 'ISM Manager')
                ship_data['manager'] = {'name': manager_info.get('company'),
                                        'imo': manager_info.get('imo'),  # IMO of company
                                        'address': manager_info.get('address'),
                                        'date_from': self._parse_doa(manager_info.get('doa'))
                                        }
            except StopIteration:
                pass

            try:
                owner_info = next(x for x in management_raw if x['role'] == 'Registered owner')
                ship_data['owner'] = {'name': owner_info.get('company'),
                                      'imo': owner_info.get('imo'),  # IMO of company
                                      'address': owner_info.get('address'),
                                      'date_from': self._parse_doa(owner_info.get('doa')),
                                      }
            except StopIteration:
                pass

        if list(ship_data.keys()) == ['imo']:
            pass

        return ship_data
