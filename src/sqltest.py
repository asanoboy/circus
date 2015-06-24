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


if __name__ == '__main__':
    with wkdb_session(Base) as session:
        print(' start ============')
        parents = [Parent(name='parent_%s' % (str(i),)) for i in range(0, 10)]
        children = [Child(name='child_%s' % (str(i),)) for i in range(0, 10)]
        children2 = [Child(name='child_%s' % (str(i),)) for i in range(0, 10)]

        print('start ============')
        session.add_all(parents)
        print('added parents ============')
        session.add_all(children)
        session.add_all(children2)
        print('added children ============')
        for i, parent in enumerate(parents):
            if i <= 2:
                continue
            parent.children = [
                c for j, c in enumerate(children)
                if j > 2 and j % i == 0]

        session.flush()
        print('deleted =================')

        child = session.query(Child).filter(Child.name == 'child_6').first()
        print(child.id, child.name)
        print([p.name for p in child.parents])
