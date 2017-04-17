def is_chinese(uchar):
    """判断一个unicode是否是汉字"""
    if uchar >= u'\u4e00' and uchar <= u'\u9fa5':
        return True
    else:
        return False


def remove_chinese_character(mix_str):
    tmp = ''
    for i in mix_str:
        if not is_chinese(i) and i not in [')','(']:
            tmp+=i
    return tmp

def only_chinese_character(mix_str):
    tmp=''
    for i in mix_str:
        if is_chinese(i) and i not in [')', '(', '）', '（', '牌']:
            tmp+=i
    return tmp

print(only_chinese_character('aa(a冷死)了死(了ccc代购ddd牌）'))

