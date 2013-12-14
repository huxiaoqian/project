# -*- coding: utf-8 -*-

from config import db

__all__ = ['SentimentCount', 'SentimentKeywords', 'TopWeibos', 'Domain']


class SentimentCount(db.Model):
    range = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    ts = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True), primary_key=True)
    count = db.Column(db.BigInteger(20, unsigned=True))

class SentimentKeywords(db.Model):
    range = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    limit = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    ts = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True), primary_key=True)
    kcount = db.Column(db.Text)

class TopWeibos(db.Model):
    range = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    limit = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    ts = db.Column(db.BigInteger(10, unsigned=True), primary_key=True)
    sentiment = db.Column(db.Integer(1, unsigned=True), primary_key=True)
    weibos = db.Column(db.Text)

class Domain(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(20), unique=True)
    zhname = db.Column(db.String(20), unique=True)
    active = db.Column(db.Boolean)