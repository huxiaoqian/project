# -*- coding: utf-8 -*-

from config import db
from model import DomainUser


FIELDS_VALUE = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea']
FIELDS_ZH_NAME = [u'文化', u'教育', u'娱乐', u'时尚', u'财经', u'媒体', u'体育', u'科技', u'海外']
FIELDS2ID = {}
FIELDS2ZHNAME = {}

for key in FIELDS_VALUE:
    idx = FIELDS_VALUE.index(key)
    FIELDS2ID[key] = idx
    FIELDS2ZHNAME[key] = FIELDS_ZH_NAME[idx]


def _union_domain(domain):
    domain_list = [domain]
    for _domain in FIELDS_VALUE:
	if _domain != domain:
	    domain_list.append(domain + ',' + _domain)
    
    return domain_list


def _domain_user(domain, top):
    domain_list = _union_domain(domain)
    domain_uids = []

    for domain in domain_list:
        items = db.session.query(DomainUser).filter(DomainUser.domains==str(domain)).all()
        domain_uids.extend([item.userId for item in items])

    return domain_uids[len(domain_uids)-top:]


if __name__ == '__main__':
    _domain_user('finance')