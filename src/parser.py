
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

