# -*- coding: utf-8 -*-
import scrapy
import pkgutil
from pkg_resources import resource_string
from ..items import scrapy01Item
import json
import os


class scrapy01Item(scrapy.Item):
	CocNumber = scrapy.Field()
	Name = scrapy.Field()
	Street = scrapy.Field()
	HouseNumber = scrapy.Field()
	PostalCode = scrapy.Field()
	City = scrapy.Field()
	PhoneNumber = scrapy.Field()
	WebsiteUrl = scrapy.Field()
	FoundingDate = scrapy.Field()
	Branch = scrapy.Field()
	LegalForm = scrapy.Field()
	VatNumber = scrapy.Field()
	Sector = scrapy.Field()
	EmailAddress = scrapy.Field()
	FaxNumber = scrapy.Field()
	LinkedIn = scrapy.Field()
	Facebook = scrapy.Field()
	Twitter = scrapy.Field()
	LogoFileName = scrapy.Field()
	Purpose = scrapy.Field()
	RSIN = scrapy.Field()
	CreationCity = scrapy.Field()
	BranchCode = scrapy.Field()
	Province = scrapy.Field()
	Employees = scrapy.Field()

class OpencompaniesAPISpider(scrapy.Spider):
	name = "opencompaniesapi3"
	allowed_domains = ["opencompanies.nl"]
	
	data = resource_string("scrapy01", "resources/kvknummers.txt")
	lines = data.split('\n')

	start_urls = []
	for l in lines:
		url = 'https://api2.opencompanies.nl/CompanyProfile/{}'.format(l.strip())
		start_urls.append(url)

	json_filter_1 = [
		"CocNumber",
		"Name",
		"Street",
		"HouseNumber",
		"PostalCode",
		"City",
		"PhoneNumber",
		"WebsiteUrl",
		"FoundingDate",
		"Branch",
		"LegalForm",
		"VatNumber",
		"Sector",
		"EmailAddress",
		"FaxNumber",
		"LinkedIn",
		"Facebook",
		"Twitter",
		"LogoFileName",
		"Purpose",
		"RSIN",
		"CreationCity",
		"BranchCode",
	]

	province = {
		"A" : "Groningen",
		"B" : "Friesland",
		"D" : "Drenthe",
		"E" : "Overijssel",
		"G" : "Gelderland",
		"H" : "Zuid-Holland",
		"L" : "Noord-Holland",
		"K" : "Limburg",
		"M" : "Utrecht",
		"P" : "Noord-Brabant",
		"S" : "Zeeland",
		"X" : "Flevoland",
	}

	def parse(self, response):
		url_origin = response.url
		print('** {}'.format(url_origin))

		out = scrapy01Item()
		rjson = json.loads(response.body_as_unicode())
		# print(rjson)
		out = { k:v for k,v in rjson.items() if k in self.json_filter_1}
		out['Employees'] = int(rjson['CountrySpecificProperties']['Employees'])
		p = rjson['CountrySpecificProperties']['Province']
		out['Province'] = self.province.get(p,p)
		# print(out)
		yield out
		"""
		if not os.path.exists('json'):
			os.makedirs('json')
		with open('json/{}.json'.format(url_origin.split('/')[-1]), 'w') as wfile:
			json.dump(out, wfile)
		"""
