
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
    id = False
    title = ''
    info = False
    text = False
    contentlength = 0
    def __init__(self, id, title, contentlength, info):
        self.title = title
        self.id = id
        self.info = info
        self.contentlength = contentlength


        
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


