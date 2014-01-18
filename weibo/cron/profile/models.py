# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, String, BigInteger, Boolean, Text, Date
from database import Base

class ProfilePersonBasic(Base):
    __tablename__ = 'profile_person_basic_test'
    id = Column(Integer, primary_key=True, autoincrement=True)
    userId = Column(BigInteger(11), unique=True)
    province = Column(Integer)
    city = Column(Integer)
    verified = Column(Boolean)
    name = Column(String(50))
    gender = Column(String(2))
    profileImageUrl = Column(String(100))
    verifiedType = Column(Integer)
    friendsCount = Column(BigInteger(20))
    followersCount = Column(BigInteger(20))
    statuseCount = Column(BigInteger(20))    
    location = Column(String(20))
    description = Column(Text)
    created_at = Column(BigInteger(10))
    date = Column(Date)

    def __init__(self, userId=None, province=None, city=None, verified=None, \
                 name=None, gender=None, profileImageUrl=None, verifiedType=None, \
                 friendsCount=None, followersCount=None, statuseCount=None, \
                 location=None, description=None, created_at=None, \
                 date=None):
        self.userId = userId
        self.province = province
        self.city = city
        self.verified = verified
        self.name = name
        self.gender = gender
        self.profileImageUrl = profileImageUrl
        self.verifiedType = verifiedType
        self.friendsCount = friendsCount
        self.followersCount = followersCount
        self.statuseCount = statuseCount
        self.location = location
        self.description = description
        self.created_at = created_at
        self.date = date

'''
class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True)
    email = Column(String(120), unique=True)

    def __init__(self, name=None, email=None):
        self.name = name
        self.email = email

    def __repr__(self):
        return '<User %r>' % (self.name)
'''
