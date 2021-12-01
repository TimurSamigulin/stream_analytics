from pathlib import Path
from parser.scraper import Scrapper

def get_profile_url():
    """
        Парсит все профили из топ-250 для каждого языка
    """
    scrapper = Scrapper()
    output_path = Path(__file__).parent.parent.parent / 'data' / 'all_chanels.csv'
    scrapper.get_streamers_profile_urls(output_path=output_path)

    # Вызываем функцию которая допарсит те страницы что пропустились во время первого парсинга
    exept_input_file = Path(__file__).parent.parent.parent / 'data' / 'except_page.txt'
    scrapper.parse_exept_chanels_url(exept_input_file, chanels_output_file=output_path)