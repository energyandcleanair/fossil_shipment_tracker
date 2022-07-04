import requests
from bs4 import BeautifulSoup

from base.env import get_env
from base.utils import to_list


class Equasis():

    session = None

    def __init__(self,
                 username=get_env('EQUASIS_EMAIL'),
                 password=get_env('EQUASIS_PASSWORD')):
        self.session = requests.Session()
        self._login(username, password)
    
    def _login (self,
                username=get_env('EQUASIS_EMAIL'),
                password=get_env('EQUASIS_PASSWORD')):
        url = 'https://www.equasis.org/EquasisWeb/authen/HomePage?fs=HomePage'
        headers = {'User-Agent': 'Mozilla/5.0'}
        payload = {'j_email':username,'j_password':password, "submit":'Login'}
        try:
            resp = self.session.post(url,headers=headers,data=payload)
        except Exception as e:
            self._log('Error logging in to equasis.org')
            #transform to your exception
            raise e

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

    def get_ships_infos(self, imos, itry=1):
        url = "https://www.equasis.org/EquasisWeb/restricted/ShipInfo?fs=Search"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = []
        for imo in to_list(imos):
            ship_data = {}
            ship_data['imo'] = imo
            payload = {
                "P_IMO": imo
            }
            
            resp = None
            try:
                resp = self.session.post(url,headers=headers,data=payload)
            except Exception as e:
                self._log("Error getting response")

                #parse it the way you want
                raise e

            html_obj = BeautifulSoup(resp.content, "html.parser")

            #In case ship info is required
            #info_box = html_obj.body.find('div', attrs={'class':'info-details'})
            if (not html_obj or not html_obj.body)and itry == 1:
                self._login()
                return self.get_ships_infos(imos=imos, itry=itry + 1)

            if not html_obj or not html_obj.body:
                return None

            pni_div = html_obj.body.find('div', attrs={'id': 'collapse6'})
            if pni_div:
                ship_data['pni'] = self._find_pni(pni_div)

            management_div = html_obj.body.find('div', attrs={'id': 'collapse3'})
            ship_data['management'] = []
            if management_div:
                ship_data['management'].append(self._find_management(management_div))

            response.append(ship_data)
                
        return response

    def get_insurer(self, imo):
        resp = self.get_ships_infos(imos=[imo])
        if resp:
            return resp[0].get('pni')
        else:
            return None

