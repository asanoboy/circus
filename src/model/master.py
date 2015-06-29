from sqlalchemy import Table, Column, Integer, ForeignKey, \
    String, Float, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


feature_item_table = Table(
    'feature_item_table', Base.metadata,
    Column('feature_id', Integer, ForeignKey('feature.id'), primary_key=True),
    Column('item_id', Integer, ForeignKey('item.id'), primary_key=True)
    )

feature_relation_table = Table(
    'feature_relation_table', Base.metadata,
    Column('id_from', Integer, ForeignKey('feature.id'), primary_key=True),
    Column('id_to', Integer, ForeignKey('feature.id'), primary_key=True)
    )


class FeatureItemAssoc(Base):
    __tablename__ = 'feature_item'

    feature_id = Column(Integer, ForeignKey('feature.id'), primary_key=True)
    item_id = Column(Integer, ForeignKey('item.id'), primary_key=True)
    strength = Column(Float)
    # TODO index


class FeatureRelationAssoc(Base):
    __tablename__ = 'feature_relation'

    id_from = Column(Integer, ForeignKey('feature.id'), primary_key=True)
    id_to = Column(Integer, ForeignKey('feature.id'), primary_key=True)
    strength = Column(Float)
    # TODO index


class Item(Base):
    __tablename__ = 'item'

    id = Column(Integer, primary_key=True, autoincrement=True)
    visible = Column(Boolean)

    features = relationship(
        'Feature',
        secondary=feature_item_table,
        backref='items')


class Page(Base):
    __tablename__ = 'page'

    id = Column(Integer, primary_key=True, autoincrement=True)
    page_id = Column(Integer)
    lang = Column(String(10))
    name = Column(String(256))

    item_id = Column(Integer, ForeignKey('item.id'))
    item = relationship('Item', uselist=False, backref='pages')

    popularity = Column(Float, nullable=False, default=0)
    viewcount = Column(Integer, nullable=False)

    def load_from_wikidb(self, lang_db):
        if self.name is not None and \
                self.lang is not None and \
                self.viewcount is not None:
            return
        if self.page_id is None:
            raise Exception('Can\'t load without page_id')
        if self.lang is None:
            self.lang = lang_db.lang
        elif self.lang != lang_db.lang:
            raise Exception('Can\'t load from another db')

        rt = lang_db.selectOne('''
            select p.name, pc.count
            from an_page p
            left join an_pagecount pc on pc.page_id = p.page_id
                and pc.year = 2014
            where p.page_id = %s
            ''', args=(self.page_id,))
        if rt is None:
            raise Exception('Can\'t load with page_id=%s.' % (self.page_id,))
        self.name = rt['name']
        self.viewcount = rt['count'] if rt['count'] is not None else 0


class Feature(Base):
    __tablename__ = 'feature'

    id = Column(Integer, primary_key=True)
    ref_item_id = Column(Integer, ForeignKey('item.id'))
    ref_item = relationship('Item', uselist=False)

    year = Column(Integer)

    features_to = relationship(
        'Feature',
        secondary=feature_relation_table,
        primaryjoin=id == feature_relation_table.c.id_from,
        secondaryjoin=id == feature_relation_table.c.id_to,
        backref='features_from')
