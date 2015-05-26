
class Category:
    id = False
    name = False

    def __init__(self, id, name):
        self.id = id
        self.name = name

class PageInfo:
    name = False
    keyValue = {}
    def __init__(self, name, keyValue):
        self.name = name
        self.keyValue = keyValue

class Page:
    contentlength = 0
    def __init__(self, id, title, contentlength, info):
        self.id = id
        self.title = title
        self.contentlength = contentlength
        self.info = info
        
    @staticmethod
    def _findInfobox(text):
        startPos = text.find("{{Infobox")
        if( startPos == -1 ):
            return False
        currentPos = startPos + 2;
        depth = 0
        while(1):
            startBracketPos = text.find("{{", currentPos)
            endBracketPos = text.find("}}", currentPos)
            if( endBracketPos == -1 ):
                print(text)
                raise

            if( startBracketPos > 0 and startBracketPos < endBracketPos ):
                depth += 1
                currentPos = startBracketPos + 2
            elif( depth==0 ):
                return text[startPos: endBracketPos+2]
            else:
                depth -= 1
                currentPos = endBracketPos + 2
        raise

def createPageInfoByBracketText(text, allowedNames=False):
    pos = text.find('|')
    if pos == -1: 
        return False

    name = text[:pos].strip().replace(' ', '_') \
        .replace('　', '_') # multibyte space
    
    if allowedNames != False:
        if name not in allowedNames:
            return False

    text = text[pos+1:]
    keyValue = { elems[0].strip(): elems[1].strip() for elems in \
        [ part.split('=') for part in text.split('|') if part.find('=')>=0 ] \
        if len(elems) == 2 }
    return PageInfo(name, keyValue)
    

