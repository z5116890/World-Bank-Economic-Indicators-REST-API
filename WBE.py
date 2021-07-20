import json
import requests
import datetime
import pandas as pd
#Flask is an API of Python that allows us to build up web-applications
from flask import Flask
#Flask-RESTX is an extension for Flask that adds support for quickly building REST APIs.
from flask_restx import Resource, Api, reqparse
import sqlite3
import re

app = Flask(__name__)
api = Api(app, default="worldbank", title="worldbank Dataset", description="worldbank api by Austin Lam (z5116890)")



post_parser = reqparse.RequestParser()
get_parser = reqparse.RequestParser()
get_sorted_parser = reqparse.RequestParser()
#will look in url query string for indicator id
post_parser.add_argument('indicator_id', required=True, location='args')
get_parser.add_argument('order_by', required=False, location='args')
get_sorted_parser.add_argument('q', required=False, location='args')

#You can provide class-wide documentation using the doc parameter of Api.route()
@api.route('/collections')
class collections(Resource):

    #The @api.response() decorator allows you to document the known responses and is a shortcut for @api.doc(responses='...').
    @api.response(201, 'Collection Created Successfully')
    @api.response(404, 'Incorrect Indicator')
    @api.response(400, 'Validation Error')
    #The @api.expect() decorator allows you to specify the expected input fields.
    @api.expect(post_parser)
    def post(self):
        #retrieve indicator_id
        indicator_id = post_parser.parse_args()['indicator_id']
        #connect to database
        conn = sqlite3.connect("z5116890.db")
        c = conn.cursor()
        #check if indicator already in db.. if yes, return 404 collection already exists
        c.execute("SELECT 1 FROM Collections WHERE indicator = ?", (indicator_id,))
        if c.fetchone():
            #return error message
            return {"message" :"The collection with indicator {} ALREADY exists in the database".format(indicator_id)}, 400

        #get data from web api
        data = requests.get("http://api.worldbank.org/v2/countries/all/indicators/" + str(indicator_id) + "?date=2012:2017&format=json&per_page=1000")
        #need to return 404 if indicator does not exist in web API
        try:
            dataJSON = data.json()[1]
        except IndexError as err:
            return {"message" :"Indicator does not exist"}, 404

        #get ready to import into db
        #get creation time
        import_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        #insert into Collections first
        c.execute("INSERT INTO Collections (creation_time, indicator, indicator_value) VALUES (?,?,?)", (import_time, dataJSON[0]['indicator']['id'], dataJSON[0]['indicator']['value']))
        #retrieve the last recent primary id to store as Entries foreign key to collections(id)
        id = c.lastrowid
        #create a list of tuples
        entries = []
        for entry in dataJSON:
            #add entry if value is not null
            if entry['value'] is not None:
                entries.append((id, entry['country']['value'], entry['date'], entry['value']))
        #use execute many to insert many tuples
        c.executemany("INSERT INTO Entries (id, country, date, value) VALUES (?,?,?,?)", entries)
        #commit to database
        conn.commit()
        #return collection data
        return {"uri" : "/collections/{}".format(id),
                "id" : "{}".format(id),
                "creation_time": "{}".format(import_time),
                "indicator_id" : "{}".format(indicator_id)}, 201


    @api.response(200, 'Collection Found Successfully')
    @api.response(400, 'Bad Response')
    @api.response(404, 'Collection Not Found')
    @api.expect(get_parser)
    def get(self):
        #get order by parameter
        order_by = get_parser.parse_args()['order_by']
        #if not None, get rid of brackets if any
        if order_by:
            order_by = re.sub("[\{\}]", "", order_by)
        #connect to database
        conn = sqlite3.connect("z5116890.db")
        c = conn.cursor()
        #create query
        query = "SELECT id, creation_time, indicator FROM Collections"

        #if no order_by, retreive collections with no order
        if order_by is None:
            c.execute("SELECT id, creation_time, indicator FROM Collections")
        #if yes...
        else:
            #concat order_by string to query
            query += " ORDER BY {}".format(order_by)
            #try and execute, if error then the given attributes in query is incorrect
            try:
                c.execute(query)
            except sqlite3.Error as err:
                return {"message" :"Incorrect attributes"}, 400

        #query successfully executed.. fetch all collections
        all_collections = c.fetchall()
        #if nothing, no collections found..
        if not all_collections:
            return {"message" :"No collections found"}, 404

        #append all collections in json format into a list
        collections = []
        for row in all_collections:
            id, creation_time, indicator_id = row
            collections.append({"uri" : "/collections/{}".format(id)
                             , "id" : "{}".format(id)
                             , "creation_time" : "{}".format(creation_time)
                             ,"indicator_id" : "{}".format(indicator_id)})

        return collections, 200



@api.route('/collections/<int:id>')
@api.param('id', 'Collection ID')
class collection(Resource):
    @api.response(404, 'Collection not found')
    @api.response(200, 'Successful')
    @api.doc(description="Delete a Collection by its ID")
    def delete(self, id):
        #connect to database
        conn = sqlite3.connect("z5116890.db")
        c = conn.cursor()
        #check if id exists.. if yes, return 1. If no, return nothing
        c.execute("SELECT 1 FROM Collections WHERE id = ?", (id,))
        if not c.fetchone():
            #return error message
            return {"message" :"The collection {} could NOT be found in the database".format(id)}, 404
        #found yes! Delete Collection and entries with same collection id
        c.execute("DELETE FROM Collections WHERE id = ?", (id,))
        c.execute("DELETE FROM Entries WHERE id = ?", (id,))
        #commit to db
        conn.commit()
        #return success message
        return {"message" :"The collection {} was removed from the database!".format(id),
                "id": "{}".format(id)}, 200

    @api.response(404, 'Collection not found')
    @api.response(200, 'Successful')
    @api.doc(description="Get a Collection by its ID")
    def get(self, id):
        #connect to database
        conn = sqlite3.connect("z5116890.db")
        c = conn.cursor()
        #check if id exists.. if yes, return 1. If no, return nothing
        c.execute("SELECT 1 FROM Collections WHERE id = ?", (id,))
        if not c.fetchone():
            #return error message
            return {"message" :"The collection {} could NOT be found in the database".format(id)}, 404

        #found yes! Get Collection
        c.execute("SELECT indicator, indicator_value, creation_time FROM Collections WHERE id = ?", (id,))
        indicator, indicator_value, creation_time = c.fetchone()

        #get all entries if any..
        entries = []
        c.execute("SELECT country, date, value FROM Entries WHERE id = ?", (id,))
        all_entries = c.fetchall()
        #check if empty.. if yes, entries is empty list, if not, append all entries into a list
        if all_entries:
            for entry in all_entries:
                country, date, value = entry
                entries.append({"country" : "{}".format(country)
                                 , "date" : "{}".format(int(date))
                                 , "value" : "{}".format(value)})

        #append entries into collections
        collection = {"id" : "{}".format(id)
                    , "indicator" : "{}".format(indicator)
                    , "indicator_value" : "{}".format(indicator_value)
                    ,"creation_time" : "{}".format(creation_time)
                    ,"entries" : entries}

        return collection, 200


@api.route('/collections/<int:id>/<string:year>/<string:country>')
@api.param('id', 'Collection ID')
@api.param('year', 'Year')
@api.param('country', 'Country')
class country_indicator_value(Resource):

    @api.response(404, 'Not found')
    @api.response(200, 'Successful')
    @api.doc(description="Get a Collection by its ID")
    def get(self, id, year, country):
        #connect to database
        conn = sqlite3.connect("z5116890.db")
        c = conn.cursor()
        #execute query
        c.execute("select value from Entries where id = ? and date = ? and country= ?", (id, year, country))
        value = c.fetchone()
        #check if empty.. if yes, entries is empty list, if not, return no entries found
        if not value:
            return {"message" :"The indicator value for Collection id {} for country {} in year {} could NOT be found".format(id, country, year)}, 404

        #get indicator_id
        c.execute("select indicator from Collections where id = ?", (id,))
        indicator_id = c.fetchone()
        if not indicator_id:
            return {"message" :"The collection of id {} could NOT be found".format(id)}, 404

        return {"id" : "{}".format(id)
              , "indicator" : "{}".format(indicator_id[0])
              , "country" : "{}".format(country)
              , "year" : "{}".format(int(year))
              , "value" : "{}".format(value[0])}, 200


@api.route('/collections/<int:id>/<string:year>')
@api.param('id', 'Collection ID')
@api.param('year', 'Year')
class sorted_countries(Resource):

    @api.response(404, 'Collection was not found')
    @api.response(400, 'Input parameter incorrect')
    @api.response(200, 'Successful')
    @api.doc(description="Get a Collection by its ID")
    @api.expect(get_sorted_parser)
    def get(self, id, year):
        #connect to database
        conn = sqlite3.connect("z5116890.db")
        c = conn.cursor()
        #connect to database)
        #check if id exists.. if yes, return 1. If no, return nothing
        c.execute("SELECT 1 FROM Collections WHERE id = ?", (id,))
        if not c.fetchone():
            #return error message
            return {"message" :"The collection {} could NOT be found in the database".format(id)}, 404

        #found yes! Get Collection
        c.execute("SELECT indicator, indicator_value FROM Collections WHERE id = ?", (id,))
        indicator, indicator_value = c.fetchone()

        #get order by parameter
        top_or_bottom = get_sorted_parser.parse_args()['q']
        #if not None, get rid of arrows if any
        if top_or_bottom:
            top_or_bottom = re.sub("[\<\>]", "", top_or_bottom)
        #if no top_or_bottom ordering, retreive collection and entries with no order
        if top_or_bottom is None:
            c.execute("select country, value from Entries where id = ? and date = ?", (id, int(year)))
        #if yes...
        else:
            #try and execute, if error then the given parameter in query is in incorrect format
            try:
                #check if + or just a number
                if top_or_bottom[0] == '+' or re.match("^\d+$", top_or_bottom):
                    top_or_bottom = re.sub("[\+]", "", top_or_bottom)
                    if int(top_or_bottom) > 100: top_or_bottom = 100
                    c.execute("select country, value from (select * from Entries where id = {} and date = \'{}\' order by value DESC limit {})".format(id, int(year), top_or_bottom))
                #then -
                else:
                    top_or_bottom = top_or_bottom[1:]
                    if int(top_or_bottom) > 100: top_or_bottom = 100
                    c.execute("select country, value from (select * from Entries where id = {} and date = \'{}\' order by value ASC limit {}) order by value DESC".format(id, int(year), top_or_bottom))
            #catch error and return incorrect query parameter format
            except sqlite3.Error as err:
                print(top_or_bottom[0])
                return {"message" :"Incorrect query parameter for sorting..."}, 400


        entries = []
        all_entries = c.fetchall()
        #check if empty.. if yes, entries is empty list, if not, append all entries into a list
        if all_entries:
            for entry in all_entries:
                country, value = entry
                entries.append({"country" : "{}".format(country)
                              , "value" : "{}".format(value)})

        #append entries into collections
        collection = {"indicator" : "{}".format(indicator)
                    , "indicator_value" : "{}".format(indicator_value)
                    ,"entries" : entries}

        return collection, 200


#create database and tables
#SCHEMA:
#   Collection {
#       id Integer PRIMARY KEY,
#       creation_time Date,
#       indicator text,
#       indicator_value text
#   }
#   Entries {
#       id Integer REFERENCES Collection(id),
#       country text,
#       date text,
#       value Float,
#   }
if __name__ == '__main__':
    #create database
    conn = sqlite3.connect("z5116890.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS Collections
                 (id Integer PRIMARY KEY, creation_time Date, indicator text, indicator_value text);""")
    c.execute("""CREATE TABLE IF NOT EXISTS Entries
                 (id Integer, country text, date Integer, value Float, FOREIGN KEY(id) REFERENCES Collections(id));""")

    # run the application
    #app.run(debug=True)
    app.run(debug=True, host='0.0.0.0', port=5432)
