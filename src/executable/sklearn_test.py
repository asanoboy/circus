# from sklearn.feature_extraction.text import CountVectorizer as Vectorizer
from sklearn.feature_extraction.text import TfidfVectorizer as Vectorizer
from config import dbhost, user, pw
import scipy as sp
from dbutils import WikiDB
import nltk.stem


def dist_raw(v1, v2):
    delta = v1 - v2
    return sp.linalg.norm(delta.toarray())


def dist_norm(v1, v2):
    v1_normalized = v1/sp.linalg.norm(v1.toarray())
    v2_normalized = v2/sp.linalg.norm(v1.toarray())
    delta = v1_normalized - v2_normalized
    return sp.linalg.norm(delta.toarray())


def generate_content(lang, infotype):
    db = WikiDB(lang)
    names = [
        'The_Chemical_Brothers',
        'Deep_Forest',
        'Basement_Jaxx',
        'Michael_Jackson',
        'Shpongle',
        'Arcade_Fire',
        ]
    return (db.createPageByTitle(name).text for name in names)


english_stemmer = nltk.stem.SnowballStemmer('english')


class StemmedCountVectorizer(Vectorizer):
    def build_analyzer(self):
        analyzer = super(StemmedCountVectorizer, self).build_analyzer()
        return lambda doc: (english_stemmer.stem(w) for w in analyzer(doc))


if __name__ == '__main__':
    # db = WikiDB('en')
    db = WikiDB('en', user, dbhost, pw)
    names = [
        'The_Chemical_Brothers',
        'Deep_Forest',
        # 'Basement_Jaxx',
        # 'Michael_Jackson',
        # 'Shpongle',
        # 'Arcade_Fire',
        # 'The_Beatles',
        # 'Dubfire',
        # 'The_Velvet_Underground',
        'Lil_Wayne',
        'Lysergic_acid_diethylamide',
        ]
    # names = list([r['name'] for r in db.generate_records_from_sql('''
    #     select name from an_page
    #     where infotype=%s limit 1000
    #     ''', arg=('Infobox_musical_artist',))])

    contents = [db.createPageByTitle(name).text for name in names]
    # contents = [c[:(int)(len(c)*0.2)] for c in contents]
    contents = [c[:3000] for c in contents]
    target = contents[0]

    # vectorizer = CountVectorizer(min_df=1, stop_words='english')
    vectorizer = StemmedCountVectorizer(
        min_df=1,
        stop_words='english',
        token_pattern=r'(?u)\b[\w_]+\b')
    X_train = vectorizer.fit_transform(contents)
    target_vec = vectorizer.transform([target])
    print(target_vec)

    for i, content in enumerate(contents):
        content_vec = X_train.getrow(i)
        # d = dist_raw(content_vec, target_vec)
        d = dist_norm(content_vec, target_vec)
        print('=== Content %i with dist=%.2f: %s' % (i, d, names[i]))

    # print(vectorizer.get_feature_names())
    # print(vectorizer.get_params())
    # print(dir(vectorizer))
