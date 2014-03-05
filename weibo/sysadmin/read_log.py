# -*- coding: UTF-8 -*-
import codecs
import re
import os
import time

def read_log(path, filename, lines):
    result = []
    n = int(lines)
    for line in codecs.open("%s/%s.log" % (path, filename)):

        if len(result) < n:
            result.append(line)
        else:
            for i in range(1,len(result)):
                result[i-1] = result[i]
            result[len(result)-1] = line

    return result
    
if __name__ == '__main__':
    main('check_clean_nocustomize_topic.log',3)
