from __future__ import division
import requests
import scraperwiki
import simplejson as json
from datetime import datetime
import boto
import os

AWS_KEY = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET_ACCESS_KEY']

total_projects = 1669
r = requests.get("https://biocollect.ala.org.au/ws/project/search?offset=0")

if 'total' in r.json():
	print r.json()['total']
	total_projects = int(r.json()['total'])


# total projects 1669

pages = total_projects//20

print(pages)

# Fetching the URL with requests

# print(len(r.json()['projects']))

for x in xrange(0,pages):
	print(x)
	offset = x * 20
	query = "https://biocollect.ala.org.au/ws/project/search?offset={offset}".format(offset=offset)
	r = requests.get(query)
	for project in r.json()['projects']:
		projectId = project['projectId']
		# print("saving " + projectId)
		data = {"projectId":project['projectId'],"data":json.dumps(project)}
		print(data)
		scraperwiki.sqlite.save(unique_keys=["projectId"], data=data, table_name="rawData")


query = '* from rawData'
results = scraperwiki.sqlite.select(query)

newData = []

keys = ["projectId",
"name",
"keywords",
"aim",
"task",
"status",
"organisationName",
"description",
"getInvolved",
"urlWeb",
"difficulty",
"scienceType",
"ecoScienceType",
"hasParticipantCost",
"equipment",
"plannedStartDate",
"plannedEndDate",
"endDate",
"coverage",
"links",
"startDate",
"url"]


for result in results:

	data = json.loads(result['data'])
	
	if data['plannedEndDate'] == None and data['isSciStarter'] == False:
		print data
		newItem = {}
		for key in keys:
			if key in data:
				if key != "coverage":
						newItem[key] = data[key]
				else:
					if 'centre' in data['coverage']:
						newItem['location'] = data['coverage']['centre']
					if 'state' in data['coverage']:
						newItem['states'] = data['coverage']['state']
					if 'lga' in data['coverage']:
						newItem['lga'] = data['coverage']['lga']	

		newData.append(newItem)	


	if data['plannedEndDate'] != None:
		if data['isSciStarter'] == False and datetime.now() < datetime.strptime(data['plannedEndDate'], '%Y-%m-%dT%H:%M:%SZ'):
			print('yep')
			newItem = {}
			for key in keys:
				if key in data:
					if key != "coverage":
						newItem[key] = data[key]
					else:
						if 'centre' in data['coverage']:
							newItem['location'] = data['coverage']['centre']
						if 'state' in data['coverage']:
							newItem['states'] = data['coverage']['state']
						if 'lga' in data['coverage']:
							newItem['lga'] = data['coverage']['lga']	
								

			newData.append(newItem)

# print newData

print(len(newData))

# with open('projects.json', 'w') as fileOut:
# 	fileOut.write(json.dumps(newData, indent=4))

print "Connecting to S3"
conn = boto.connect_s3(AWS_KEY,AWS_SECRET)
bucket = conn.get_bucket('gdn-cdn')

from boto.s3.key import Key

k = Key(bucket)
k.key = "2018/10/aus-cit-sci/projects.json"
k.set_metadata("Cache-Control", "max-age=90")
k.set_metadata("Content-Type", "application/json")
k.set_contents_from_string(json.dumps(newData))
k.set_acl("public-read")

print "JSON is updated"
	