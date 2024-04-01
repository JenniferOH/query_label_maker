import re


def ends_with_jong(kstr):
    m = re.search("[가-힣0-9]+", kstr)
    if m:
        k = m.group()[-1]
        return (ord(k) - ord("가")) % 28 > 0
    else:
        return


def _ulul(kstr):
    josa = "을" if ends_with_jong(kstr) else "를"
    # print(f"{kstr}{josa} ", end='')
    return josa


def _yika(kstr):
    josa = "이" if ends_with_jong(kstr) else "가"
    # print(f"{kstr}{josa} ", end='')
    return josa


def _wakwa(kstr):
    josa = "과" if ends_with_jong(kstr) else "와"
    # print(f"{kstr}{josa} ", end='')
    return josa


def _eunun(kstr):
    josa = "은" if ends_with_jong(kstr) else "는"
    # print(f"{kstr}{josa} ", end='')
    return josa


def _and(kstr, is_final=False):
    if ends_with_jong(kstr):
        return
    josa = "고" if ends_with_jong(kstr) else "이고"


if __name__ == '__main__':
    print(_yika('lang'))


