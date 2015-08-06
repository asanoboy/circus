from gensim import corpora, models, similarities
from envutils import init_logger, get_wiki_db
from debug import get_logger
from itertools import groupby

def create_data(use_tfidf=False):
    lang = 'en'
    db = get_wiki_db(lang)

    all_records = db.selectAndFetchAll('''
        select
            pl.id_from, pl.id_to, pl.odr,
            p_from.page_title name_from,
            p_to.page_title name_to
        from an_pagelinks_musical_artist_for_topic pl
        inner join page p_from on p_from.page_id = pl.id_from
        inner join page p_to on p_to.page_id = pl.id_to
        order by id_from
        ''')
    
    records_groupby = groupby(all_records, lambda x: x['id_from'])

    texts = [[r['name_to'] for r in records]
            for id_from, records
            in records_groupby]

    dic = corpora.Dictionary(texts)
    id2word = {id: w for w, id in dic.token2id.items()}
    # corpus = [dictionary.doc2bow(text) for text in texts]
    corpus = [
        [(dic.token2id[r['name_to']], int(1+r['odr'])) for r in records]
        for id_from, records
        in records_groupby]
    if use_tfidf:
        tfidf = models.TfidfModel(corpus)
        corpus = list(tfidf[corpus])
    return dic, corpus, id2word


def load_models():
    return models.LsiModel.load('lsi_raw.dat'), \
        models.LsiModel.load('lsi_tfidf.dat'), \
        models.LdaModel.load('lda_raw.dat'), \
        models.LdaModel.load('lda_tfidf.dat')

if __name__ == '__main__':
    init_logger()
    logger = get_logger(__name__)
    
    for use_tfidf in [True, False]:
        suffix = 'tfidf' if use_tfidf else 'raw'

        with logger.lap('prepare'):
            dictionary, corpus, id2word = create_data(use_tfidf)

        dictionary.save('data_%s.dict' % (suffix,))

        with logger.lap('lsi'):
            lsi = models.LsiModel(
                corpus=corpus,
                num_topics=30,
                id2word=dictionary)
        lsi.save('lsi_%s.dat' % (suffix,))

        with logger.lap('lda'):
            lda = models.LdaModel(
                corpus=corpus,
                num_topics=30,
                id2word=dictionary)
        lda.save('lda_%s.dat' % (suffix,))
