from sqlalchemy import Table, Column, Integer, ForeignKey, \
    String
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()

feature_item_table = Table(
    'feature_item', Base.metadata,
    Column('left_id', Integer, ForeignKey('feature.id')),
    Column('right_id', Integer, ForeignKey('item.id'))
    )


class Page(Base):
    __tablename__ = 'page'

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Item(Base):
    __tablename__ = 'item'

    id = Column(Integer, primary_key=True)
    page_id = Column(Integer, ForeignKey('page.id'))
    page = relationship('Page', uselist=False)

    features = relationship(
        'Feature',
        secondary=feature_item_table,
        backref='items')


class Feature(Base):
    __tablename__ = 'feature'

    id = Column(Integer, primary_key=True)
    ref_item_id = Column(Integer, ForeignKey('item.id'))
    ref_item = relationship('Item', uselist=False)

    year = Column(Integer)
    popularity = Column(Integer)
