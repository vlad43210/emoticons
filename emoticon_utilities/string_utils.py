'''utils file for emoticon processing'''

def normalizeEmoticonName(emoticon):
    norm_name = ""
    for echar in emoticon:
        if echar == ':': norm_name += 'colon_'
        elif echar == ')': norm_name += 'rparen_'
        elif echar == '(': norm_name += 'lparen_'
        elif echar == '^': norm_name += 'carrot_'
        elif echar == '_': norm_name += 'underscore_'
        elif echar == '>': norm_name += 'greaterthan_'
        elif echar == '<': norm_name += 'lessthan_'
        elif echar == '.': norm_name += 'dot_'
        elif echar == ';': norm_name += 'semicolon_'
    return norm_name