
def getBracketTexts(text):
    result = []
    currentPos = 0
    startPos = -1
    depth = 0
    nextStartPos = text.find("{{", currentPos)
    nextEndPos = text.find("}}", currentPos)
    try:
        while 1:
            if nextStartPos == -1 and nextEndPos == -1:
                if depth != 0:
                    raise 'Unclosed info bracket.'
                break
            elif nextEndPos == -1: # exists only {{
                raise 'Unclosed info bracket.'
            elif nextStartPos == -1: # exists only }}
                if depth != 1:
                    raise 'Too many "}}" at last'
                    break
                result.append( text[startPos+2: nextEndPos] )
                break
            else: # exists both {{ and }}
                if nextStartPos < nextEndPos:
                    if depth==0:
                        startPos = nextStartPos
                    depth += 1
                    currentPos = nextStartPos + 2
                    nextStartPos = text.find("{{", currentPos)
                else:
                    if depth==0:
                        raise 'Too many "}}"'
                    elif depth==1:
                        if startPos == -1:
                            raise 'Invalid'
                        result.append( text[startPos+2: nextEndPos] )
                        startPos = -1
                    depth -= 1
                    currentPos = nextEndPos + 2
                    nextEndPos = text.find("}}", currentPos)
    except:
        print("Can't parse wiki text.")
        pass # workaround
    return result

def removeComment(text):
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
