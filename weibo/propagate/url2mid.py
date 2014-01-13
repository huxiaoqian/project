#-*- encoding: utf-8 -*-
import re
ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
a='http://weibo.com/1630461754/Arw04j3LW?ref=home'
b='weibo.com/1630461754/Arw04j3LW?ref=home'
c='weibo.com/1630461754/Arw04j3LW'

def base62_decode(string, alphabet=ALPHABET):
    """Decode a Base X encoded string into the number
 
    Arguments:
    - `string`: The encoded string
    - `alphabet`: The alphabet to use for encoding
    """
    base = len(alphabet)
    strlen = len(string)
    num = 0
 
    idx = 0
    for char in string:
        power = (strlen - (idx + 1))
        num += alphabet.index(char) * (base ** power)
        idx += 1
 
    return num

def url_to_mid(url):
    '''
    >>> url_to_mid('z0JH2lOMb')
    3501756485200075L
    >>> url_to_mid('z0Ijpwgk7')
    3501703397689247L
    >>> url_to_mid('z0IgABdSn')
    3501701648871479L
    >>> url_to_mid('z08AUBmUe')
    3500330408906190L
    >>> url_to_mid('z06qL6b28')
    3500247231472384L
    >>> url_to_mid('yCtxn8IXR')
    3491700092079471L
    >>> url_to_mid('yAt1n2xRa')
    3486913690606804L
    '''
    url = str(url)[::-1]
    size = len(url) / 4 if len(url) % 4 == 0 else len(url) / 4 + 1
    result = []
    for i in range(size):
        s = url[i * 4: (i + 1) * 4][::-1]
        s = str(base62_decode(str(s)))
        s_len = len(s)
        if i < size - 1 and s_len < 7:
            s = (7 - s_len) * '0' + s
        result.append(s)
    result.reverse()
    return int(''.join(result))

def get_mid(url):
    mid=''
    pat1 = re.compile(r'https://weico.com/')
    pat2 = re.compile(r'http://weibo.com/')
    pat3 = re.compile(r'weibo.com/')
    pat4 = re.compile(r'\?ref=home')
    url=re.sub(pat1,'',url)
    url=re.sub(pat2,'',url)
    url=re.sub(pat3,'',url)
    url=re.sub(pat4,'',url)
    k=url.split('/')
    for i in range(len(k)):
        try:
            k[i]=int(k[i])
        except Exception,e:
            print e
        finally:
            if isinstance(k[i],int):
                if i<len(k)-1:
                    mid = k[i+1]
    part=mid.split('?')
    mid=part[0]
    return int(url_to_mid(mid))


        


