import re
import requests
import time
import logging
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

class Scrapper():

    def get_languages(self) -> set:
        """
             Функция возвращаяет список языков стримеров
             return: set - множество языков в видел строк
        """

        base_url = 'https://twitchtracker.com/channels/viewership'
        
        try:
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
            r = requests.get(base_url, headers=headers)
        except OSError:
            print(f'OSError: {base_url}')

        soup = BeautifulSoup(r.text, 'lxml')
        

        lang_selector = soup.find('select', id='lang-selector')
        oprions = lang_selector.find_all('option')
        
        languages = []
        for option in oprions:
            languages.append(option.text)

        languages = languages[1:]
        return set(languages)

    def get_viewership_tr(self, url: str) -> list:
        """
            Возвращает список строк из таблицы с каналами
            params:
            url: str - Ссылка на странице
            return: list - Список soup 
        """
        try:
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
            r = requests.get(url, headers=headers)
        except OSError:
            print(f'OSError: {url}')
        try:
            soup = BeautifulSoup(r.text, 'lxml')
            table = soup.find('table', id='channels')
            tbody = table.find('tbody')
        except:
            logger.info('Exeption 503')
            except_page = Path(__file__).parent.parent.parent / 'data' / 'except_page.txt'
            with open(except_page, 'a', encoding='utf-8') as fw:
                fw.write(url + '\n')
            return []


        chanels = tbody.findAll('tr')
        return chanels

    def get_chanels_urls(self, url: str, lang: str, min_avg_count: int = 300) -> list:
        """
            Функция для парсинга каналов со страницы с каналами
            params:
            url: str - page link
            lang: str - sreamers language
            min_avg_count - минимальное кол-во средних зрителей за месяц при котором мы парим канал  
        """
        chanel_urls = []

        chanels = self.get_viewership_tr(url)

        if not chanels:
            return []

        for chanel in chanels:
            chanel_info = chanel.findAll('td')
            try:
                avg_view = chanel_info[3].text
                chanel_info = chanel_info[2].find('a')
            except IndexError: # Пропускаем строки зарезервированные для рекламы
                continue

            # avg_view = int(re.sub(r'\D', '', avg_view))

            # if avg_view < min_avg_count:
            #     return chanel_urls

            chanel_urls.append({'title': chanel_info.text, 'url': chanel_info['href'], 'languages': lang})
        
        return chanel_urls


    def get_streamers_profile_urls(self, output_path: Path = None) -> list:
        """
            Собирает профили стримеров на сайте twitchtracker
            params:
            output_path: Path = None - если передать путь, то сохранит в файл результаты в виде csv
        """
        base_url = 'https://twitchtracker.com/channels/most-views/{}/personality?page={}'
        languages = self.get_languages()
        all_chanels = []

        for lindex, lang in enumerate(languages):
            lang_chanels = []
            for page in range(1, 6):
                logging.info(f'language {lindex+1}/{len(languages)}- {lang}, page - {page}/5 ')
                time.sleep(1)
                url = base_url.format(lang.lower(), page)
                chanels = self.get_chanels_urls(url, lang, min_avg_count=0)
                
                if not chanels:
                    time.sleep(5)
                    continue

                if len(chanels) < 50:
                    lang_chanels += chanels
                    break
                    
                lang_chanels += chanels
            all_chanels += lang_chanels

        if output_path:
            df = pd.DataFrame(all_chanels)
            df.to_csv(output_path)
            del df
        
        return all_chanels
    
    def parse_exept_chanels_url(self, 
                                exept_input_file: Path,
                                chanels_output_file: Path, 
                                new_chanels_output_file: Path = None):
        """
            Функция допарсит те страницы которые пропустились во время основного парсинга
            params:
            exept_input_file: Path - Файл .txt с сылками на каждой строке с страницами которые не удалось спарсить 
            chanels_output_file: Path - файл где сохранены спаршенные профили
            new_chanels_output_file: Path - файл, куда записать объединеный dataframe допаршеных профилей, если не передан,
                                            то перезапишет chanels_output_file
        """
        if not exept_input_file.is_file():
            return None
        
        if not new_chanels_output_file:
            new_chanels_output_file = chanels_output_file

        all_exept_chanels = []

        with open(exept_input_file, 'r', encoding='utf-8') as fr:
            for profile in fr.readlines():
                lang = profile.split('/')[-2]
                chanels = self.get_chanels_urls(profile, lang)
                all_exept_chanels += chanels

        df = pd.read_csv(chanels_output_file, index_col=0, header=0)
        df1 = pd.DataFrame(all_exept_chanels)
        df2 = pd.concat([df, df1], ignore_index=True)

        df2.to_csv(new_chanels_output_file)

        exept_input_file.unlink()
            



if __name__ == '__main__':
    scrapper = Scrapper()

    




