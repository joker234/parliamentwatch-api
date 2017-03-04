import requests
import re
from bs4 import BeautifulSoup
from datetime import datetime
from collections import defaultdict
from pprint import pprint
import json
from pymongo import MongoClient

def get_parliaments():
	return requests.get("https://abgeordnetenwatch.de/api/parliaments.json").json()['parliaments'] # TODO URL auslagern

def get_profiles(parliament):
	if type(parliament) == dict:
		parliament = parliament['uuid']
	r = requests.get("https://www.abgeordnetenwatch.de/api/parliament/%s/profiles.json" % parliament) # TODO URL auslagern
	r.raise_for_status()
	return r.json()['profiles']

def get_deputies(parliament):
	if type(parliament) == dict:
		parliament = parliament['uuid']
	r = requests.get("https://www.abgeordnetenwatch.de/api/parliament/%s/deputies.json" % parliament) # TODO URL auslagern
	r.raise_for_status()
	return r.json()['profiles']

def get_polls(parliament):
	if type(parliament) == dict:
		parliament = parliament['uuid']
	r = requests.get("https://www.abgeordnetenwatch.de/api/parliament/%s/polls.json" % parliament) # TODO URL auslagern
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
			base_url = "https://www.abgeordnetenwatch.de" # TODO auslagern
			return base_url+relative_url

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
		r = requests.get("http://www.abgeordnetenwatch.de/profile/public_questions.php?build=1&num=%i&cmd=%i&id=%i" % (page, cmd, id))
		print("http://www.abgeordnetenwatch.de/profile/public_questions.php?build=1&num=%i&cmd=%i&id=%i" % (page, cmd, id))
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

	# TODO muss wieder raus, nur testweise
	with open('dump_answers.json', 'w') as f:
		pprint(result, f)

	return result

def parliaments2mongo(db, wished_parliaments):
	parliaments = [x for x in get_parliaments() if x['name'] in wished_parliaments]

	db.parliaments.insert_many(parliaments)
	return

def deputies2mongo(db, parliaments):
	for parliament in parliaments:
		print(parliament['name'], parliament['uuid'])
		try:
			deputies = get_deputies(parliament['uuid'])
		except requests.exceptions.HTTPError:
			print("404 not found")
		else:
			db.profiles.insert_many(deputies)
		print(parliament['name'], parliament['uuid'])
	return

def polls2mongo(db, parliaments):
	for parliament in parliaments:
		print(parliament['name'], parliament['uuid'])
		try:
			polls = get_polls(parliament['uuid'])
		except requests.exceptions.HTTPError:
			print("404 not found")
		else:
			db.polls.insert_many(polls)
		print(parliament['name'], parliament['uuid'])
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
			print("No profile found for uuid: %s, name: %s" % (uuid, n[uuid]))
		else:
			profile['votes'] = d[uuid]
			db.profiles.save(profile)

def q_a2mongo(db, profiles):
	i = 1 # TODO debug entfernen
	for profile in profiles:
		# pprint(profile)
		print(i, profile['personal']['last_name']+", "+profile['personal']['first_name'])
		i += 1 # TODO debug fnord mit 731 als Endzahl

		questions = get_questions(profile)
		profile['questions'] = questions
		db.profiles.find_one_and_replace({"meta.uuid": profile['meta']['uuid']}, profile)

		print("    ", profile['meta']['uuid'])

if __name__ == '__main__':
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

	# Profiles of Deputies
	deputies2mongo(db, db.parliaments.find())

	# polls of Deputies
	polls2mongo(db, db.parliaments.find())

	# Q/A of Profiles
	q_a2mongo(db, db.profiles.find())
	# q_a2mongo(db, db.profiles.find({"personal.last_name": "Müller", "personal.first_name": "Norbert"}))
