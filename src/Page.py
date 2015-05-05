
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

    @staticmethod
    def _removeComment(text):
        currentPos = 0
        commentRanges = []
        text = text.replace('<-->', '')
        while 1:
            startPos = text.find("<!--", currentPos)
            endPos = text.find("-->", currentPos)
            if startPos == -1 or endPos == -1:
                break
            elif startPos < endPos:
                commentRanges.append((startPos, endPos + 3))
                currentPos = endPos + 3
            elif startPos > endPos:
                commentRanges.append((endPos, endPos + 3))
                currentPos = endPos + 3
            # elif startPos >= 0:
            #     commentRanges.append((startPos, startPos + 3))
            #     currentPos = startPos + 3
            # elif endPos >= 0:
            #     commentRanges.append((endPos, endPos + 4))
            #     currentPos = endPos + 4
            else:
                raise

        textRanges = []
        for i, r in enumerate(commentRanges):
            if( i==0 ):
                textRanges.append((0, r[0]))
            else:
                textRanges.append((commentRanges[i-1][1], r[0]) )

            if( i==len(commentRanges)-1 ):
                textRanges.append((r[1], len(text)))

        if len(textRanges) > 0:
            result = ''
            for r in textRanges:
                result += text[r[0]: r[1]]
            return result
        else:
            return text

