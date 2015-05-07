from main import *
import re

def allInfoRecordGenerator(openConn):
    for cols in selectGenerator(openConn, 'anadb.info_ex', cols=['text_id', 'name'], order='text_id asc'):
        yield {'text_id':cols[0], 'name':cols[1]}

def updateInfoRedirect():
    infoRecordIter = allInfoRecordGenerator(openConn)
    p = re.compile('#redirect\s*\[\[(template:)?\s*(.+)\]\]', re.IGNORECASE)
    for infoRecord in infoRecordIter:
        infoName = infoRecord['name']
        text = selectTextByTitle(infoName, 10)
        m = p.search(text)
        if m:
            redirectName = m.group(2).replace(' ', '_')
            cur.execute("""
                select text_id from anadb.info_ex where name = %s
            """, (redirectName,))
            result = cur.fetchall()

            if len(result) == 0:
                cur.execute("""
                    select text_id from anadb.info_ex where lower(name) = lower(%s)
                """, (redirectName,))
                result = cur.fetchall()
                
            if len(result) == 1:
                redirectTo = result[0]['text_id']
                cur.execute("""
                    update anadb.info_ex set redirect_to = %s
                    where text_id = %s
                """, (redirectTo, infoRecord['text_id']))
            else:
                print( 'Invalid redirect from %s to %s' % (infoName, redirectName) )

        else:
            if text.lower().startswith('#redirect') :
                msg = 'Missing to parse redirect info "%s".' % (text,)
                print(msg)
                raise Exception(msg)
    conn.commit()

