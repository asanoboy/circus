def pos_in_content(name, content):
    pos = content.find(name)
    if pos != -1:
        return pos

    return content.find(' '.join(name.split('_')))


class Category:
    id = False
    name = False

    def __init__(self, id, name):
        self.id = id
        self.name = name


class PageInfo:
    def __init__(self, name, keyValue):
        self.name = name
        self.keyValue = keyValue


class Page:
    def __init__(self, id, title, text, info):
        self.id = id
        self.title = title
        self.contentlength = len(text)
        self.text = text
        self.info = info

    @staticmethod
    def _findInfobox(text):
        startPos = text.find("{{Infobox")
        if(startPos == -1):
            return False
        currentPos = startPos + 2
        depth = 0
        while 1:
            startBracketPos = text.find("{{", currentPos)
            endBracketPos = text.find("}}", currentPos)
            if endBracketPos == -1:
                print(text)
                raise

            if startBracketPos > 0 and startBracketPos < endBracketPos:
                depth += 1
                currentPos = startBracketPos + 2
            elif depth == 0:
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
        .replace('　', '_')  # multibyte space

    if allowedNames is not False:
        if name not in allowedNames:
            return False

    key_value_list = []
    start_pos = pos+1
    search_pos = start_pos
    while 1:
        separator_pos = text.find('|', search_pos)
        if separator_pos == -1:
            break

        curly_pos = text.find('{{', search_pos)
        if curly_pos != -1 and curly_pos < separator_pos:
            end_pos = text.find('}}', curly_pos)
            if end_pos >= 0:
                search_pos = end_pos
                continue
            else:
                print('Not found "}}" in : ', text)

        brace_pos = text.find('[[', search_pos)
        if brace_pos != -1 and brace_pos < separator_pos:
            end_pos = text.find(']]', brace_pos)
            if end_pos >= 0:
                search_pos = end_pos
                continue
            else:
                print('Not found "]]" in : ', text)

        key_value_list.append(text[start_pos: separator_pos])
        start_pos = separator_pos + 1
        search_pos = start_pos

    parts = [part.split('=') for part in key_value_list if part.find('=') >= 0]
    keyValue = {
        elems[0].strip(): '='.join(elems[1:]).strip()
        for elems in parts}
    return PageInfo(name, keyValue)
