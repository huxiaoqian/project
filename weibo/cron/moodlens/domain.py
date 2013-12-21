# -*- coding: utf-8 -*-

from config import db
from model import Domain


FIELDS_VALUE = ['culture', 'education', 'entertainment', 'fashion', 'finance', 'media', 'sports', 'technology', 'oversea']
FIELDS_ZH_NAME = [u'文化', u'教育', u'娱乐', u'时尚', u'财经', u'媒体', u'体育', u'科技', u'海外']
FIELDS2ID = {}
FIELDS2ZHNAME = {}

for key in FIELDS_VALUE:
    idx = FIELDS_VALUE.index(key)
    FIELDS2ID[key] = idx
    FIELDS2ZHNAME[key] = FIELDS_ZH_NAME[idx]


def _is_domain_active(domain):
    domain = Domain.query.filter_by(idx=domain).first()
    if domain:
        return domain.active
    else:
        return False


def _domains_active():
    domains = []
    items = Domain.query.filter_by(active=True).all()

    for item in items:
        domains.append({'idx': item.idx, 'name': item.name, 'zhname': item.zhname})

    return domains


def _domain_id(domain):
    domain = Domain.query.filter_by(name=domain)
    if domain:
        return domain.id
    else:
        return None


def _domain_en_name(domainid):
    domain = Domain.query.filter_by(idx=domainid).first()
    if domain:
        print domain.name
        return domain.name
    else:
        return None


def _maintain_domain():
    for name in FIELDS_VALUE:
        idx = FIELDS2ID[name]
        zhname = FIELDS2ZHNAME[name]
        active = True

        item = Domain(idx, name, zhname, active)
        item_exist = Domain.query.filter_by(idx=idx, name=name, zhname=zhname).first()

        if item_exist:
            db.session.delete(item_exist)
            db.session.add(item)
        else:
            db.session.add(item)

    db.session.commit()
    count = Domain.query.count()

    print 'maintain domain over, there is %s domains now' % count


if __name__ == '__main__':
	_maintain_domain()
