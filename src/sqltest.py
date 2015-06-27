from builders.builder_utils import wkdb_session
from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

assoc_table = Table('assoc', Base.metadata,
    Column('parent_id', Integer, ForeignKey('parent.id')),
    Column('child_id', Integer, ForeignKey('child.id'))
    )


class Parent(Base):
    __tablename__ = 'parent'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    children = relationship('Child',
        secondary=assoc_table,
        backref='parents')


class Child(Base):
    __tablename__ = 'child'

    id = Column(Integer, primary_key=True)
    name = Column(String)


class Other(Base):
    __tablename__ = 'other'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    parent_id = Column(Integer, ForeignKey('parent.id'))
    parent = relationship('Parent', uselist=False)

    child_id = Column(Integer, ForeignKey('child.id'))
    child = relationship('Child', uselist=False)


if __name__ == '__main__':
    with wkdb_session(Base) as session:
        print(' start ============')

        def dump():
            print('============= DUMP >>')
            parents = session.query(Parent).all()
            for p in parents:
                print(p.name)
                for c in p.children:
                    print('     =>', c.name)
            print('<< ==================')

        p = Parent(id=1, name='firstP')
        c = Child(id=1, name='firstC')
        p.children.append(c)
        session.add(p)
        session.add(c)
        session.flush()

        dump()

        p = Parent(id=2, name='secondP')
        c = Child(id=2, name='secondC')
        o = Other(id=1, name='secondO', parent=p, child=c)

        session.add(p)
        session.flush()

        dump()

