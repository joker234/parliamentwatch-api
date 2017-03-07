import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime
from collections import defaultdict
from locale import setlocale, atof, LC_NUMERIC
import logging
from os import environ
import sys
from pprint import pprint
import json
from pymongo import MongoClient

global BASE_URL_HTTPS
BASE_URL_HTTPS = "https://www.abgeordnetenwatch.de" # TODO Variable an sinnvolle stelle
global BASE_URL_HTTP
BASE_URL_HTTP = "http://www.abgeordnetenwatch.de" # TODO Variable an sinnvolle stelle

def get_parliaments():
	return requests.get("%s/api/parliaments.json" % BASE_URL_HTTPS).json()['parliaments']

def get_profiles(parliament):
	if type(parliament) == dict:
		parliament = parliament['uuid']
	r = requests.get("%s/api/parliament/%s/profiles.json" % (BASE_URL_HTTPS, parliament)) # TODO URL auslagern
	r.raise_for_status()
	return r.json()['profiles']

def get_deputies(parliament):
	if type(parliament) == dict:
		parliament = parliament['uuid']
	r = requests.get("%s/api/parliament/%s/deputies.json" % (BASE_URL_HTTPS, parliament)) # TODO URL auslagern
	r.raise_for_status()
	return r.json()['profiles']

def get_polls(parliament):
	if type(parliament) == dict:
		parliament = parliament['uuid']
	r = requests.get("%s/api/parliament/%s/polls.json" % (BASE_URL_HTTPS, parliament)) # TODO URL auslagern
	r.raise_for_status()
	return r.json()['polls']

def get_alternativ_profile_url(r,parliament):
	s = BeautifulSoup(r.text, "html.parser")
	legends = s.find_all("legend")
	legends = [l for l in legends if l.fieldset != None]
	for l in legends:
		inner_legend = l.fieldset.legend
		if(inner_legend.span.text == parliament['name']):
			relative_url = inner_legend.next_sibling.next_sibling.find("a", class_="link-profile")['href']
			return BASE_URL_HTTPS+relative_url

def get_cmd_id(profile_url,parliament):
	r = requests.get(profile_url, allow_redirects=False)
	try:
		match = re.match('^.*-(\d*)-(\d*).html$', r.headers['Location'])
	except KeyError:
		profile_url = get_alternativ_profile_url(r,parliament) # TODO Ist das so sinnvoll?
		return get_cmd_id(profile_url,parliament)
	return (int(match.group(1)), int(match.group(2)))

def extract_meta_and_text(obj, res):
	res['title'] = obj.find("div", class_="title").text
	res['date'] = datetime.strptime(obj.find("div", class_="datum").text, "%d.%m.%Y")
	text = obj.find("div", class_="text")
	to_extract = text.find("div", class_="name")
	if to_extract is not None:
		to_extract.extract()
	res['text'] = text.text
	return

# Output format:
# [
# 	{
#		"title" : "Some Title",
#		"date" : dateobject,
#		"question" : "This is the Question."
#		"answers" :
#			[
# 				{
#					"title" : "Some Title",
#					"date" : dateobject,
#					"answer" : "This is a answer."
#			]
#	}
# ]
def get_questions(profile):
	if type(profile) == dict:
		profile_url = profile['meta']['url']
	else:
		profile_url = profile
	cmd, id = get_cmd_id(profile_url,profile['parliament'])
	result = []
	page = 1
	while True:
		logging.debug("%s/profile/public_questions.php?build=1&num=%i&cmd=%i&id=%i" % (BASE_URL_HTTP, page, cmd, id))
		r = requests.get("%s/profile/public_questions.php?build=1&num=%i&cmd=%i&id=%i" % (BASE_URL_HTTP, page, cmd, id))
		r.encoding = 'UTF-8'
		s = BeautifulSoup(r.text, "html.parser")
		if page == 1:
			browse_next = s.find("div", class_="browse next")
			if browse_next == None:
				return {}
			try:
				pagenumber = int(browse_next.find_previous("a").text)
			except ValueError:
				pagenumber = int(browse_next.find_previous("b").text)
		questions = s.find("div", class_="questions")
		for question in questions.find_all("div", class_="question"):
			qnumber = int(re.match("^q(\d*)$", question.a['name']).group(1))
			res_q = {}
			extract_meta_and_text(question, res_q)
			res_q['answers'] = []
			answers = s.find_all("div", id="bookmark_%i" % qnumber)
			for answer in answers:
				answer = answer.find("div", class_="answer")
				res_a = {}
				try:
					extract_meta_and_text(answer, res_a)
				except AttributeError:
					pass
				res_q['answers'].append(res_a)
			result.append(res_q)

		# do while replacement
		if page < pagenumber:
			page += 1
		else:
			break
	return result

def typecast_deputies(deputies):
	for deputy in deputies:
		logging.debug("Typecasting: %s, %s" % (deputy['personal']['last_name'], deputy['personal']['first_name']))
		# meta
		deputy['meta']['status'] = int(deputy['meta']['status']) # 2016-10-06 14:40
		deputy['meta']['edited'] = datetime.strptime(deputy['meta']['edited'], "%Y-%m-%d %H:%M")
		# personal
		deputy['personal']['birthyear'] = int(deputy['personal']['birthyear'])
		# parliament
		if len(deputy['parliament']) > 0:
			try:
				deputy['parliament']['joined'] = datetime.strptime(deputy['parliament']['joined'], "%Y-%m-%d")
			except ValueError:
				pass
			try:
				deputy['parliament']['retired'] = datetime.strptime(deputy['parliament']['retired'], "%Y-%m-%d")
			except ValueError:
				pass
		# constituency
		if len(deputy['constituency']) > 0:
			deputy['constituency']['number'] = int(deputy['constituency']['number'])
			if deputy['constituency']['result'] != None:
				deputy['constituency']['result'] = float(atof(deputy['constituency']['result']))
		# list
		if len(deputy['list']) > 0:
			if deputy['list']['position'] != None:
				deputy['list']['position'] = int(deputy['list']['position'])

def parliaments2mongo(db, wished_parliaments):
	logging.info("Getting parliaments.")
	parliaments = [x for x in get_parliaments() if x['name'] in wished_parliaments]

	db.parliaments.insert_many(parliaments)
	logging.info("Getting parliaments. (done)")
	return

def deputies2mongo(db, parliaments, locale="de_DE.utf-8"):
	setlocale(LC_NUMERIC, locale)
	for parliament in parliaments:
		logging.info("Getting deputies of: %s %s" % (parliament['name'], parliament['uuid']))
		try:
			deputies = get_deputies(parliament['uuid'])
		except requests.exceptions.HTTPError:
			logging.error("404 not found")
		else:
			typecast_deputies(deputies)
			db.profiles.insert_many(deputies)
		logging.info("Getting deputies of: %s %s (done)" % (parliament['name'], parliament['uuid']))
	return

def polls2mongo(db, parliaments):
	for parliament in parliaments:
		logging.info("Getting Polls of: %s %s" % (parliament['name'], parliament['uuid']))
		try:
			polls = get_polls(parliament['uuid'])
		except requests.exceptions.HTTPError:
			logging.error("404 not found")
		else:
			db.polls.insert_many(polls)
		logging.info("Getting Polls of: %s %s (done)" % (parliament['name'], parliament['uuid']))
	update_mongo_votes_meta(db)
	return

def update_mongo_votes_meta(db):
	d = defaultdict(lambda: defaultdict(int))
	n = {}
	for poll in db.polls.find():
		for deputy in poll['votes']:
			uuid = deputy['uuid']
			vote = deputy['vote']
			n[uuid] = deputy['name']
			if vote == 'dafür gestimmt':
				d[uuid]['yes'] += 1
			elif vote == 'dagegen gestimmt':
				d[uuid]['no'] += 1
			elif vote == 'enthalten':
				d[uuid]['abstention'] += 1
			elif vote == 'nicht beteiligt':
				d[uuid]['missed'] += 1
			else:
				raise ValueError
	for uuid in d:
		profile = db.profiles.find_one({"meta.uuid": uuid})
		if profile == None:
			logging.warning("No profile found for uuid: %s, name: %s" % (uuid, n[uuid]))
		else:
			profile['votes'] = d[uuid]
			db.profiles.save(profile)

def q_a2mongo(db, profiles):
	logging.info("Getting Q/A of the given profiles.")
	logging.info("This takes a while. Please be patient and wait ☺")
	DEBUG = logging.getLogger().getEffectiveLevel() == logging.DEBUG
	if DEBUG:
		i = 1
	for profile in profiles:
		if DEBUG:
			logging.debug("%i %s, %s" % (i, profile['personal']['last_name'], profile['personal']['first_name']))
			i += 1

		questions = get_questions(profile)
		profile['questions'] = questions
		profile['meta']['questions'] = len(questions)
		answers = 0
		for question in questions:
			for answer in question['answers']:
				if len(answer) > 0:
					answers += 1
		profile['meta']['answers'] = answers
		logging.debug("Updated q/a counter: %i questions, %i answers" % (len(questions), answers))
		db.profiles.update_one({"meta.uuid": profile['meta']['uuid']}, {"$set": profile})

		logging.debug("    %s" % profile['meta']['uuid'])
	logging.info("Getting Q/A of the given profiles. (done)")

def initialize_logging(level=None, defaultLevel="INFO", datefmt=None):
	if not level:
		gettrace = getattr(sys, 'gettrace', None)
		if environ.get('LOG_LEVEL') in logging._nameToLevel.keys():
			level = environ.get('LOG_LEVEL')
		elif gettrace is not None:
			if gettrace():
				level = "DEBUG"
			else:
				level = defaultLevel
		else:
			level = defaultLevel
	logging.basicConfig(format='%(asctime)s %(message)s', datefmt=datefmt, level=level)

if __name__ == '__main__':
	# logging init
	initialize_logging()

	# TODO write function for mongodb connection
	mongodb = MongoClient()
	db = mongodb.test_db

	mongodb.drop_database(db)

	# Parliaments
	wished_parliaments = [
		'Bundestag',
		# 'Baden-Württemberg',
		# 'Bayern',
		# 'Berlin',
		# 'Brandenburg',
		# 'Bremen',
		# 'Hamburg',
		# 'Hessen',
		# 'Mecklenburg-Vorpommern',
		# 'Niedersachsen',
		# 'Nordrhein-Westfalen',
		# 'Rheinland-Pfalz',
		# 'Saarland',
		# 'Sachsen',
		# 'Sachsen-Anhalt',
		# 'Schleswig-Holstein',
		# 'Thüringen'
	]
	parliaments2mongo(db, wished_parliaments)
	parliaments = list(db.parliaments.find(modifiers={"$snapshot": True}))

	# Profiles of Deputies
	deputies2mongo(db, parliaments)

	# polls of Deputies
	polls2mongo(db, parliaments)

	# Q/A of Profiles
	q_a2mongo(db, db.profiles.find(modifiers={"$snapshot": True}))
