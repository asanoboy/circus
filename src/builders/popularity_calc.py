from model.master import Page


def generate_popularity():
    popularity = 1000
    ratio = 2
    num = 10
    while 1:
        for i in range(num):
            yield popularity

        num *= ratio
        popularity -= 1


class PopularityCalc:
    def __init__(self, master, lang_dbs):
        self.master = master
        self.lang_to_db = {d.lang: d for d in lang_dbs}

    def build(self):
        pages = self.master.query(Page)
        pages = sorted(pages, key=lambda p: -p.viewcount)

        current_popularity = None
        last_count = None
        for page, popularity in zip(pages, generate_popularity()):
            count = page.viewcount
            if last_count != count:
                current_popularity = popularity

            last_count = count
            page.popularity = current_popularity

        min_pop = pages[-1].popularity
        max_pop = pages[0].popularity
        for page in pages:
            pop = page.popularity
            page.popularity = \
                ((pop - min_pop) / (max_pop - min_pop)) * 100

        self.master.flush()
