# -*- coding: cp1251 -*-
# version 1.2

from pathlib import Path


def make(path=None, word=None) -> str:
    def check_legal_chars(wd):
        wd = wd[:244] if len(wd) > 244 else wd
        wl = list()
        for ch in wd:
            if ord(ch) in range(48, 58) or \
                    ord(ch) in range(65, 91) or \
                    ord(ch) in range(97, 123) or \
                    ord(ch) in [32, 33, 35, 36, 37, 38, 40, 41, 43, 44, 45, 46, 59, 61, 64, 91, 93, 94, 95, 96] or \
                    ord(ch) in range(1040, 1104) or ord(ch) in [1025, 1105]:
                wl.append(ch)
            elif ord(ch) == 8211:
                wl.append('-')
            elif ord(ch) == 8470:
                wl.append('No')
            else:
                wl.append(' ')
        wd = ''.join(map(str, wl))

        wl.clear()
        for ch in wd.split(' '):
            if len(ch) > 0:
                wl.append(ch)
        wd = ' '.join(map(str, wl))
        while True:
            if wd[-1:] in ['.', ' ']:
                wd = wd[:-1]
            elif wd[:1] in ['.', ' ']:
                wd = wd[1:]
            else:
                break
        return wd

    if word:
        return check_legal_chars(word)
    elif path:
        p = Path(path)
        parts = [x for x in p.with_suffix('').parts]
        if p.anchor:
            parts.pop(0)
        legal_path = p.anchor
        for part in parts:
            part = check_legal_chars(part)
            legal_path = Path(legal_path, part)
        return str(legal_path.with_suffix(p.suffix))


def show_rules():
    digits = []
    for ch in range(48, 58):
        digits.append({ch: chr(ch)})
    print(digits)
    eng_upper = []
    for ch in range(65, 91):
        eng_upper.append({ch: chr(ch)})
    print(eng_upper)
    eng_lower = []
    for ch in range(97, 123):
        eng_lower.append({ch: chr(ch)})
    print(eng_lower)
    rus = []
    for ch in range(1040, 1104):
        rus.append({ch: chr(ch)})
    for ch in [1025, 1105]:
        rus.append({ch: chr(ch)})
    print(rus)
    char = []
    for ch in [32, 33, 35, 36, 37, 38, 40, 41, 43, 44, 45, 46, 59, 61, 64, 91, 93, 94, 95, 96]:
        char.append({ch: chr(ch)})
    print(char)
    # forbidden_symbols = ['\0', '\a', '\b', '\t', '\v', '\r', '\n', '\f',
    #                      '/', '\\', '|', '<', '>', "'", '"', '«', '»', '?', '*', ':']


if __name__ == '__main__':
    a = 'ВМЗ-ФРАГМЕНТ «БАРЫШНЯ В СМОЛЬ-8311907101400'
    b = make(word=a)
    print(b)

    p1 = Path(
        r'a://192.168.10.102\arc_1\2021\Исходник\04_Апрель\Происшествия Итоговый выпуск\20200417_PGM_23h52_Seg709.mxf')
    print(make(path=str(p1)))
